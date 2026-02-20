import scrapy
import os
import json
import re
import pandas as pd
import dns.resolver
from urllib.parse import urlparse

class FinderSpider(scrapy.Spider):
    name = "Finder_Spider"

    custom_settings = {
        "DEPTH_LIMIT": 1,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_TIMEOUT": 15,
        "FEED_FORMAT": "csv",
        "FEED_URI": "qualified_sites.csv",
        "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    # ------------------------------------------------
    # INIT
    # ------------------------------------------------
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.domain_phones = {}  # moved here for consistency

        self.include_keywords, self.block_keywords = self.load_keywords()
        self.start_urls_list = self.load_input_files()

        self.visited = set()
        self.domain_data = {}
        self.yielded_domains = set()

    # ------------------------------------------------
    # LOAD KEYWORDS
    # ------------------------------------------------
    def load_keywords(self):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        keyword_path = os.path.join(project_root, "Keywords.json")

        if not os.path.exists(keyword_path):
            raise FileNotFoundError(f"Keywords.json not found at {keyword_path}")

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

        for file in os.listdir(input_folder):
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

            for idx, row in df.iterrows():
                # Extract URL/Website
                for col in df.columns:
                    if col.lower() in ["website", "url", "website url"]:
                        url = row[col]
                        if pd.notna(url):
                            urls.append(url)
                            domain = urlparse(url).netloc.lower().replace("www.", "")
                            # Extract phone from CSV
                            phone = None
                            for ph_col in df.columns:
                                if "phone" in ph_col.lower() or "contact" in ph_col.lower():
                                    phone = row[ph_col]
                                    break
                            if pd.notna(phone):
                                self.domain_phones[domain] = str(phone).strip()

        return list(set(urls))

    # ------------------------------------------------
    # START REQUESTS
    # ------------------------------------------------
    def start_requests(self):
        for url in self.start_urls_list:
            if not url.startswith("http"):
                url = "http://" + url

            yield scrapy.Request(
                url=url,
                callback=self.parse_page,
                meta={"depth": 0, "root_url": url}
            )

    # ------------------------------------------------
    # PARSE PAGE
    # ------------------------------------------------
    def parse_page(self, response):
        root_url = response.meta["root_url"]
        domain = urlparse(root_url).netloc.lower().replace("www.", "")

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
        filtered_emails = set()
        for e in emails:
            if not e.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".wixpress.com")):
                filtered_emails.add(e)
        for email in filtered_emails:
            self.domain_data[domain]["emails"].add(email)

        # 🎯 Evaluation & Yield (Yield as soon as criteria met)
        if not self.domain_data[domain]["blocked"] and self.domain_data[domain]["include_count"] >= 4:
            if domain not in self.yielded_domains:
                self.yielded_domains.add(domain)
                yield self.build_item(domain)

        # 🔁 BFS Traversal with optimizations
        if self.domain_data[domain]["include_count"] < 8 and response.meta["depth"] < 1:
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
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_page,
                        meta={
                            "depth": response.meta["depth"] + 1,
                            "root_url": root_url
                        }
                    )

    # ------------------------------------------------
    # DNS LOOKUP
    # ------------------------------------------------
    def dns_lookup(self, domain):
        try:
            result = dns.resolver.resolve(domain, "A")
            return ",".join([ip.address for ip in result])
        except:
            return "N/A"

    # ------------------------------------------------
    # BUILD OUTPUT
    # ------------------------------------------------
    def build_item(self, domain):
        data = self.domain_data[domain]
        phone = self.domain_phones.get(domain, "")
        self.logger.info(f"YIELDING ITEM for domain: {domain}")

        return {
            "domain": domain,
            "keyword_matches": data["include_count"],
            "emails_found": ",".join(data["emails"]),
            "phones_found": phone,  # phone from CSV
            "dns_ips": self.dns_lookup(domain)
        }