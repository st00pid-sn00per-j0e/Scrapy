import json
import os
import re
from html import unescape
from urllib.parse import unquote, urlparse

import dns.resolver
import pandas as pd
import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.utils.defer import maybe_deferred_to_future
from twisted.internet import threads
from twisted.internet.defer import DeferredList


class FinderSpider(scrapy.Spider):
    name = "Finder_Spider"

    EMAIL_REGEX = re.compile(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}",
        re.IGNORECASE,
    )
    MAILTO_REGEX = re.compile(
        r"mailto:\s*([A-Za-z0-9._%+\-]+(?:%40|@)[A-Za-z0-9.\-]+\.[A-Za-z]{2,10})",
        re.IGNORECASE,
    )
    OBFUSCATED_EMAIL_REGEX = re.compile(
        r"([A-Za-z0-9._%+\-]{1,64})\s*"
        r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\}|\s+at\s+|@)\s*"
        r"([A-Za-z0-9.\-]{1,253})\s*"
        r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\s+dot\s+|\.)\s*"
        r"([A-Za-z]{2,10})",
        re.IGNORECASE,
    )
    HEX_ESCAPE_REGEX = re.compile(r"\\x([0-9a-fA-F]{2})")
    UNICODE_ESCAPE_REGEX = re.compile(r"\\u([0-9a-fA-F]{4})")

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
        "DEPTH_LIMIT": 2,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_ENABLED": False,
        "ITEM_PIPELINES": {
            "Nizami.pipelines.QualifiedSitesCsvPipeline": 300,
        },
        "QUALIFIED_SITES_OUTPUT": "qualified_sites.csv",
        "BRUTE_EMAIL_VALIDATE_DNS": False,
    }
    
    # Extended URL patterns to follow for more pages with emails
    FOLLOW_URL_PATTERNS = [
        "contact", "about", "team", "support", "info", "email",
        "contact-us", "get-in-touch", "our-team", "staff", "people",
        "meet-the-team", "about-us", "company", "partners", "locations",
        "address", "write-to-us", "inquiry", "enquiry", "hello"
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.crawler = crawler
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

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

        self.batch_size = 10
        self.all_urls = self.start_urls_list
        self.current_batch = -1
        self.pending = 0

    def _start_batch(self, batch_index):
        start = batch_index * self.batch_size
        end = min(start + self.batch_size, len(self.all_urls))
        batch_urls = self.all_urls[start:end]
        if not batch_urls:
            return

        self.current_batch = batch_index
        self.logger.info(
            "Starting batch %s with %s URLs",
            batch_index + 1,
            len(batch_urls),
        )

        for url in batch_urls:
            request = scrapy.Request(
                url=url,
                callback=self.parse_page,
                errback=self.handle_request_error,
                meta={"depth": 0, "root_url": url, "batch_index": batch_index},
            )
            self.pending += 1
            yield request

    async def start(self):
        for request in self.start_requests():
            yield request

    def start_requests(self):
        yield from self._start_batch(0)

    def spider_idle(self, spider):
        if self.pending != 0:
            return

        next_batch = self.current_batch + 1
        if next_batch * self.batch_size >= len(self.all_urls):
            return

        self.logger.info("Spider idle, starting next batch %s", next_batch + 1)
        for request in self._start_batch(next_batch):
            try:
                self.crawler.engine.crawl(request)
            except TypeError:
                self.crawler.engine.crawl(request, spider)

        raise DontCloseSpider

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

        with open(keyword_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        include = [keyword.lower() for keyword in data.get("include_keywords", [])]
        block = [keyword.lower() for keyword in data.get("block_keywords", [])]
        return include, block

    def load_input_files(self):
        input_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Input"
        )
        urls = []

        for file in sorted(os.listdir(input_folder)):
            path = os.path.join(input_folder, file)

            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(path, sep=None, engine="python", on_bad_lines="warn")
                elif file.endswith(".xlsx") or file.endswith(".xls"):
                    df = pd.read_excel(path)
                else:
                    continue
            except Exception as error:
                self.logger.warning("Failed to read %s: %s", file, error)
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
                if (
                    col_name in ["company name", "agency name", "name", "business name"]
                    and company_column is None
                ):
                    company_column = col

            if url_column is None:
                self.logger.warning("No URL column found in %s", file)
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

    @classmethod
    def _decode_js_escapes(cls, source_text):
        if not source_text:
            return ""

        decoded = cls.HEX_ESCAPE_REGEX.sub(
            lambda match: chr(int(match.group(1), 16)),
            source_text,
        )
        decoded = cls.UNICODE_ESCAPE_REGEX.sub(
            lambda match: chr(int(match.group(1), 16)),
            decoded,
        )
        return decoded

    @classmethod
    def _extract_emails_from_source(cls, source_text):
        if not source_text:
            return set()

        blobs = []
        raw = source_text
        html_unescaped = unescape(raw)
        url_decoded = unquote(html_unescaped)
        js_decoded = cls._decode_js_escapes(url_decoded)

        for blob in (raw, html_unescaped, url_decoded, js_decoded):
            if blob and blob not in blobs:
                blobs.append(blob)

        found = set()
        for blob in blobs:
            for email in cls.EMAIL_REGEX.findall(blob):
                found.add(email.strip().lower())

            for email in cls.MAILTO_REGEX.findall(blob):
                normalized = unquote(email).strip().lower()
                normalized = normalized.replace("%40", "@")
                found.add(normalized)

            for match in cls.OBFUSCATED_EMAIL_REGEX.finditer(blob):
                local = match.group(1).strip().lower()
                domain_part = match.group(2).strip().lower().strip(".")
                tld = match.group(3).strip().lower()
                found.add(f"{local}@{domain_part}.{tld}")

        return found

    def _build_email_candidates(self, source_text):
        candidates = []
        seen = set()
        extracted = self._extract_emails_from_source(source_text)
        for email in extracted:
            normalized_email = email.strip().lower()
            normalized_email = normalized_email.replace("mailto:", "").strip(" <>\"'(),;")
            if not normalized_email or normalized_email in seen:
                continue
            if self.is_trash_email(normalized_email):
                continue
            seen.add(normalized_email)
            candidates.append(normalized_email)
        return candidates

    def handle_request_error(self, failure):
        request = failure.request
        self.pending = max(0, self.pending - 1)
        self.logger.warning(
            "Request failed: %s (%s) - pending: %s",
            request.url,
            failure.value,
            self.pending,
        )

    async def parse_page(self, response):
        root_url = response.meta["root_url"]
        domain = self.normalize_domain(root_url)
        batch_index = response.meta.get("batch_index", self.current_batch)

        try:
            if domain not in self.domain_data:
                self.domain_data[domain] = {
                    "include_count": 0,
                    "blocked": False,
                    "emails": set(),
                }

            source_text = response.text
            text = source_text.lower()

            for keyword in self.block_keywords:
                if keyword in text:
                    self.domain_data[domain]["blocked"] = True
                    return

            count = sum(text.count(keyword) for keyword in self.include_keywords)
            self.domain_data[domain]["include_count"] += count

            candidates = self._build_email_candidates(source_text)

            results = []
            if candidates:
                if self.settings.getbool("BRUTE_EMAIL_VALIDATE_DNS", False):
                    deferreds = []
                    for email in candidates:
                        deferreds.append(threads.deferToThread(self._validate_email_dns, email))
                    dlist = DeferredList(deferreds, consumeErrors=True)
                    results = await maybe_deferred_to_future(dlist)
                else:
                    results = [(True, email) for email in candidates]

            return self._after_dns_checks(results, response, domain, batch_index)
        finally:
            self.pending = max(0, self.pending - 1)
            self.logger.debug(
                "Request finished: %s - pending: %s",
                response.url,
                self.pending,
            )

    def _validate_email_dns(self, email):
        email_domain = email.rsplit("@", 1)[-1]
        if self.email_domain_has_dns(email_domain):
            return email
        return None

    def _after_dns_checks(self, results, response, domain, batch_index):
        for success, email_or_none in results:
            if success and email_or_none:
                self.domain_data[domain]["emails"].add(email_or_none)

        outputs = []

        # Lowered threshold from 4 to 2 for more sites to qualify
        if (
            not self.domain_data[domain]["blocked"]
            and self.domain_data[domain]["include_count"] >= 2
        ):
            if domain not in self.yielded_domains:
                self.yielded_domains.add(domain)
                outputs.append(self.build_item(domain))

        # Allow deeper crawling with depth limit 2
        if self.domain_data[domain]["include_count"] < 10 and response.meta["depth"] < 2:
            root_url = response.meta["root_url"]
            for link in response.css("a::attr(href)").getall():
                next_url = response.urljoin(link)

                if domain not in urlparse(next_url).netloc.lower():
                    continue

                if next_url.lower().endswith((".jpg", ".png", ".pdf", ".zip", ".gif", ".svg", ".webp")):
                    continue

                # Use the extended FOLLOW_URL_PATTERNS list
                if not any(
                    item in next_url.lower()
                    for item in self.FOLLOW_URL_PATTERNS
                ):
                    continue

                if next_url not in self.visited:
                    self.visited.add(next_url)
                    request = scrapy.Request(
                        next_url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        meta={
                            "depth": response.meta["depth"] + 1,
                            "root_url": root_url,
                            "batch_index": batch_index,
                        },
                    )
                    self.pending += 1
                    outputs.append(request)

        return outputs

    def build_item(self, domain):
        data = self.domain_data[domain]
        website_url = self.domain_websites.get(domain, f"https://{domain}")
        company = self.domain_companies.get(domain, "")
        phone = self.domain_phones.get(domain, "")
        self.logger.info("YIELDING ITEM for domain: %s", domain)

        return {
            "Website URL": website_url,
            "Company Name": company,
            "Phone Number": phone,
            "Keyword Matches": data["include_count"],
            "Emails": ",".join(sorted(data["emails"])),
        }
