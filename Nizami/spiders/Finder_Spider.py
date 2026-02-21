import scrapy
import os
import json
import re
import pandas as pd
import dns.resolver
from urllib.parse import urlparse
from twisted.internet import threads
from twisted.internet.defer import DeferredList
from scrapy.utils.defer import maybe_deferred_to_future

class FinderSpider(scrapy.Spider):
    name = "Finder_Spider"

    IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico")
    TRASH_EMAIL_DOMAINS = {
        "sentry.io",
        "example.com",
        "example.org",
        "example.net",
        "mysite.com",
        "yourdomain.com",
        "domain.com",
    }
    TRASH_EMAIL_LOCAL_PARTS = {
        "example",
        "test",
        "email",
        "user",
        "demo",
        "sample",
        "dummy",
        "null",
        "none",
        "noreply",
        "no-reply",
        "donotreply",
        "do-not-reply",
    }

    custom_settings = {
        "DEPTH_LIMIT": 1,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_ENABLED": False,
        "FEEDS": {
            "qualified_sites.csv": {
                "format": "csv",
                "encoding": "utf-8",
                "overwrite": True,
                "fields": ["Website URL", "Company Name", "Phone Number", "Keyword Matches", "Emails"],
            }
        },
    }

    # ------------------------------------------------
    # INIT
    # ------------------------------------------------
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.domain_phones = {}
        self.domain_companies = {}
        self.domain_websites = {}
        self.email_domain_dns_cache = {}
        self.dns_resolver = dns.resolver.Resolver()
        self.dns_resolver.timeout = 1.0
        self.dns_resolver.lifetime = 2.0

        self.include_keywords, self.block_keywords = self.load_keywords()
        self.start_urls_list = self.load_input_files()

        self.visited = set()
        self.domain_data = {}
        self.yielded_domains = set()

    # ------------------------------------------------
    # HELPERS
    # ------------------------------------------------
    @staticmethod
    def normalize_domain(url):
        return urlparse(url).netloc.lower().replace("www.", "").strip()

    @staticmethod
    def normalize_url(raw_url):
        if pd.isna(raw_url):
            return None

        url = str(raw_url).strip()
        if not url:
            return None

        if not url.startswith(("http://", "https://")):
            url = f"http://{url}"

        parsed = urlparse(url)
        if not parsed.netloc or " " in parsed.netloc:
            return None

        return url

    @classmethod
    def is_trash_email(cls, email):
        value = email.strip().lower()
        if "@" not in value:
            return True

        local, domain = value.rsplit("@", 1)
        domain = domain.lstrip(".")
        if domain.startswith("www."):
            domain = domain[4:]

        if not local or not domain:
            return True

        if domain in cls.TRASH_EMAIL_DOMAINS or domain.startswith("example."):
            return True
        if domain.endswith(".sentry.io") or "mysite" in domain:
            return True
        if domain.endswith("wixpress.com"):
            return True

        if local in cls.TRASH_EMAIL_LOCAL_PARTS:
            return True

        # Filters token-like mailbox names (common in JS telemetry/sentry keys)
        if re.fullmatch(r"[0-9a-f]{24,}", local):
            return True
        if re.fullmatch(r"[0-9a-f-]{30,}", local):
            return True

        tld = domain.rsplit(".", 1)[-1]
        if not re.fullmatch(r"[a-z]{2,10}", tld):
            return True

        if value.endswith(cls.IMAGE_SUFFIXES):
            return True

        return False

    def email_domain_has_dns(self, domain):
        key = domain.strip().lower()
        if not key:
            return False

        cached = self.email_domain_dns_cache.get(key)
        if cached is not None:
            return cached

        is_valid = False
        try:
            mx_records = self.dns_resolver.resolve(key, "MX")
            is_valid = bool(mx_records)
        except Exception:
            try:
                a_records = self.dns_resolver.resolve(key, "A")
                is_valid = bool(a_records)
            except Exception:
                is_valid = False

        self.email_domain_dns_cache[key] = is_valid
        return is_valid

    # ------------------------------------------------
    # LOAD KEYWORDS
    # ------------------------------------------------
    def load_keywords(self):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        keyword_path = None

        for file in os.listdir(project_root):
            if file.lower() == "keywords.json":
                keyword_path = os.path.join(project_root, file)
                break

        if not keyword_path:
            raise FileNotFoundError(
                f"Keywords.json file not found in project root: {project_root}"
            )

        with open(keyword_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        include = [k.lower() for k in data.get("include_keywords", [])]
        block = [k.lower() for k in data.get("block_keywords", [])]

        return include, block

    # ------------------------------------------------
    # LOAD INPUT FILES
    # ------------------------------------------------
    def load_input_files(self):
        input_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Input")
        urls = []

        for file in sorted(os.listdir(input_folder)):
            path = os.path.join(input_folder, file)

            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(path, sep=None, engine='python', on_bad_lines='warn')
                elif file.endswith(".xlsx") or file.endswith(".xls"):
                    df = pd.read_excel(path)
                else:
                    continue
            except Exception as e:
                self.logger.warning(f"Failed to read {file}: {e}")
                continue

            url_column = None
            phone_column = None
            company_column = None

            for col in df.columns:
                col_name = str(col).strip().lower()
                if col_name in ["website", "url", "website url"] and url_column is None:
                    url_column = col
                if ("phone" in col_name or "contact" in col_name) and phone_column is None:
                    phone_column = col
                if col_name in ["company name", "agency name", "name", "business name"] and company_column is None:
                    company_column = col

            if url_column is None:
                self.logger.warning(f"No URL column found in {file}")
                continue

            for _, row in df.iterrows():
                normalized_url = self.normalize_url(row[url_column])
                if not normalized_url:
                    continue

                urls.append(normalized_url)
                domain = self.normalize_domain(normalized_url)
                if domain not in self.domain_websites:
                    self.domain_websites[domain] = normalized_url

                if phone_column is not None:
                    phone = row[phone_column]
                    if pd.notna(phone):
                        phone_value = str(phone).strip()
                        if phone_value and phone_value.lower() != "nan":
                            self.domain_phones[domain] = phone_value

                if company_column is not None and domain not in self.domain_companies:
                    company = row[company_column]
                    if pd.notna(company):
                        company_value = str(company).strip()
                        if company_value and company_value.lower() != "nan":
                            self.domain_companies[domain] = company_value

        return list(set(urls))

    # ------------------------------------------------
    # START REQUESTS
    # ------------------------------------------------
    async def start(self):
        for request in self.start_requests():
            yield request

    def start_requests(self):
        for url in self.start_urls_list:
            yield scrapy.Request(
                url=url,
                callback=self.parse_page,
                errback=self.handle_request_error,
                meta={"depth": 0, "root_url": url}
            )

    def handle_request_error(self, failure):
        request = failure.request
        self.logger.warning(f"Request failed: {request.url} ({failure.value})")

    # ------------------------------------------------
    # PARSE PAGE
    # ------------------------------------------------
    async def parse_page(self, response):
        root_url = response.meta["root_url"]
        domain = self.normalize_domain(root_url)

        if domain not in self.domain_data:
            self.domain_data[domain] = {
                "include_count": 0,
                "blocked": False,
                "emails": set()
            }

        text = response.text.lower()

        # 🚨 BLOCK CHECK (Immediate rejection)
        for keyword in self.block_keywords:
            if keyword in text:
                self.domain_data[domain]["blocked"] = True
                return  # stop crawling this domain immediately

        # ✅ INCLUDE COUNT
        count = sum(text.count(keyword) for keyword in self.include_keywords)
        self.domain_data[domain]["include_count"] += count

        # 📧 EMAIL EXTRACTION
        emails = re.findall(
            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
            text
        )
        candidates = []
        seen_candidates = set()
        for e in emails:
            normalized_email = e.strip().lower()
            if self.is_trash_email(normalized_email):
                continue

            if normalized_email in seen_candidates:
                continue

            seen_candidates.add(normalized_email)
            candidates.append(normalized_email)

        results = []
        if candidates:
            deferreds = []
            for email in candidates:
                deferreds.append(threads.deferToThread(self._validate_email_dns, email))

            dlist = DeferredList(deferreds, consumeErrors=True)
            results = await maybe_deferred_to_future(dlist)

        return self._after_dns_checks(results, response, domain)

    def _validate_email_dns(self, email):
        email_domain = email.rsplit("@", 1)[-1]
        if self.email_domain_has_dns(email_domain):
            return email
        return None

    def _after_dns_checks(self, results, response, domain):
        for success, email_or_none in results:
            if success and email_or_none:
                self.domain_data[domain]["emails"].add(email_or_none)

        outputs = []

        # 🎯 Evaluation & Yield (Yield as soon as criteria met)
        if not self.domain_data[domain]["blocked"] and self.domain_data[domain]["include_count"] >= 4:
            if domain not in self.yielded_domains:
                self.yielded_domains.add(domain)
                outputs.append(self.build_item(domain))

        # 🔁 BFS Traversal with optimizations
        if self.domain_data[domain]["include_count"] < 8 and response.meta["depth"] < 1:
            root_url = response.meta["root_url"]
            for link in response.css("a::attr(href)").getall():
                next_url = response.urljoin(link)

                # only internal links
                if domain not in urlparse(next_url).netloc.lower():
                    continue

                # skip static resources
                if next_url.lower().endswith((".jpg", ".png", ".pdf", ".zip", ".gif")):
                    continue

                # only follow relevant pages
                if not any(x in next_url.lower() for x in ["contact", "about", "team", "support", "info", "email"]):
                    continue

                if next_url not in self.visited:
                    self.visited.add(next_url)
                    outputs.append(
                        scrapy.Request(
                            next_url,
                            callback=self.parse_page,
                            errback=self.handle_request_error,
                            meta={
                                "depth": response.meta["depth"] + 1,
                                "root_url": root_url
                            }
                        )
                    )

        return outputs

    # ------------------------------------------------
    # BUILD OUTPUT
    # ------------------------------------------------
    def build_item(self, domain):
        data = self.domain_data[domain]
        website_url = self.domain_websites.get(domain, f"https://{domain}")
        company = self.domain_companies.get(domain, "")
        phone = self.domain_phones.get(domain, "")
        self.logger.info(f"YIELDING ITEM for domain: {domain}")

        return {
            "Website URL": website_url,
            "Company Name": company,
            "Phone Number": phone,
            "Keyword Matches": data["include_count"],
            "Emails": ",".join(sorted(data["emails"])),
        }
