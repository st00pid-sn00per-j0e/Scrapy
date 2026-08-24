# #!/usr/bin/env python3
# """
# Gliner_Spider  —  FinderSpider + AI Leadership Enrichment
# ==========================================================
# - Preserves every original input column/value (only fills blanks).
# - Forces headless mode for any browser spawned by this process or its children.
# - Single output CSV written exclusively by the pipeline.
# - Batched crawling fixed: no manual pending counter that breaks idle detection.
# - Extracts ALL people (not just the most senior) as comma-separated lists.
# - Deterministic social-link extraction (Twitter, LinkedIn, Facebook, Instagram,
#   YouTube, Other) from every page.
# - Deterministic tel: link phone extraction added to a new "Phone Numbers" column.
# """

# import os

# # ═══════════════════════════════════════════════════════════
# # FORCE HEADLESS MODE for any browser / driver child process
# # ═══════════════════════════════════════════════════════════
# os.environ["SE_HEADLESS"] = "1"
# os.environ["PLAYWRIGHT_HEADLESS"] = "1"
# os.environ["CHROME_HEADLESS"] = "1"
# os.environ["MOZ_HEADLESS"] = "1"
# os.environ["PYPPETEER_HEADLESS"] = "1"
# os.environ["HEADLESS"] = "1"

# import gc
# import json
# import random
# import re
# import time
# import warnings
# import concurrent.futures
# from html import unescape
# from typing import Any, Dict, Optional
# from urllib.parse import unquote, urlparse

# import dns.resolver
# import pandas as pd
# import scrapy
# from scrapy import signals
# from scrapy.exceptions import DontCloseSpider
# from scrapy.utils.defer import maybe_deferred_to_future
# from twisted.internet import threads
# from twisted.internet.defer import DeferredList

# # Optional deps
# try:
#     import g4f
#     from g4f import Provider
#     from g4f.client import Client
#     HAS_G4F = True
# except ImportError:
#     HAS_G4F = False

# try:
#     from bs4 import BeautifulSoup
#     HAS_BS4 = True
# except ImportError:
#     HAS_BS4 = False

# warnings.filterwarnings("ignore", message="Unclosed client session")
# warnings.filterwarnings("ignore", message="Unclosed connector")


# # ============================================================
# # AI CONFIG
# # ============================================================

# AI_ENABLED = HAS_G4F

# AI_PROVIDER_NAMES = [
#     "Cloudflare",
#     "CohereForAI_C4AI_Command",
#     "AnyProvider",
#     "Gemini",
#     "Perplexity",
#     "Yqcloud",
#     "OpenaiChat",
#     "MiniMax",
#     "WhiteRabbitNeo",
# ]
# AI_FALLBACK_MODEL = "gpt-4o-mini"
# AI_PER_CALL_TIMEOUT = 35
# AI_RACE_TIMEOUT = 50
# AI_MAX_RETRIES = 5
# AI_RETRY_BASE_DELAY = 2.0
# AI_MAX_WORKERS = min(12, len(AI_PROVIDER_NAMES))
# AI_MAX_TEXT_CHARS = 8000
# AI_REQUIRED_KEYS = {"person_name", "job_title", "service", "relevancy", "sales_hook"}


# # ============================================================
# # AI HELPERS
# # ============================================================

# def _ai_clean_text(html_text: str) -> str:
#     if not html_text:
#         return ""
#     if HAS_BS4:
#         soup = BeautifulSoup(html_text, "lxml")
#         for tag in soup(["script", "style", "noscript", "svg", "iframe", "nav", "footer"]):
#             tag.decompose()
#         text = soup.get_text(separator=" ", strip=True)
#     else:
#         text = re.sub(r"<[^>]+>", " ", html_text)
#     return re.sub(r"\s+", " ", text).strip()


# def _ai_build_prompt(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> str:
#     target_str = target if target else "general business relevance"
#     example = (
#         '{"people":['
#         '{"person_name":"Alex Smith","job_title":"Founder & CEO"},'
#         '{"person_name":"Max Doe","job_title":"CTO"},'
#         '{"person_name":"Adam Lee","job_title":"COO"}'
#         '],"service":"AI recruitment software","relevancy":true,'
#         '"sales_hook":"What is the biggest bottleneck in scaling your AI recruitment platform?"}'
#     )

#     parts = [
#         "You are an expert B2B lead researcher. Extract EVERY person mentioned on this company's website.",
#         "",
#         "EXTRACTION RULES -- FOLLOW STRICTLY:",
#         "1. people: A JSON array of objects. Each object must have:",
#         "   - person_name: The FULL NAME of the person.",
#         "   - job_title: Their EXACT job title as written on the site.",
#         "   - Search carefully in the LEADERSHIP TEXT (About Us, Team, Leadership, Who We Are, Founders, Executives, Directors, Staff, Board, Management).",
#         "   - Include EVERY person you find. Do NOT limit yourself to the most senior leader.",
#         "   - If no human name appears anywhere, return an empty array [].",
#         "",
#         "2. service: What the company actually sells/does in 2-8 words. Be specific and concise.",
#         "",
#         '3. relevancy: true or false. Does this company match the target: "' + target_str + '"?',
#         "   - true = the company clearly fits the target description.",
#         "   - false = the company does not fit or is unclear.",
#         "",
#         "4. sales_hook: ONE concise, attention-grabbing first question (max 12 words) to ask the MOST SENIOR person.",
#         "   - Make it specific to their service and role.",
#         "",
#         "Respond with ONLY a single-line valid JSON object. No markdown fences, no explanation.",
#         "",
#         "Example: " + example,
#         "",
#         "=== LEADERSHIP TEXT (highest priority) ===",
#         '"""' + (leadership_text[:5000] if leadership_text else "(no leadership text scraped)") + '"""',
#         "",
#         "=== GENERAL SITE TEXT ===",
#         '"""' + (general_text[:3000] if general_text else "(no general text scraped)") + '"""',
#     ]
#     return "\n".join(parts)


# def _ai_parse_json(raw: str) -> Optional[Dict[str, Any]]:
#     if not raw:
#         return None
#     raw = raw.strip()
#     raw = re.sub(r"^```(json)?", "", raw, flags=re.IGNORECASE).strip()
#     raw = re.sub(r"```$", "", raw).strip()
#     match = re.search(r"\{.*\}", raw, re.DOTALL)
#     if not match:
#         return None
#     try:
#         data = json.loads(match.group(0))
#     except json.JSONDecodeError:
#         return None

#     if not isinstance(data, dict):
#         return None

#     result = {}

#     # Handle array of people and flatten to comma-separated strings
#     people_list = data.get("people", [])
#     if isinstance(people_list, list) and people_list:
#         names = []
#         titles = []
#         for p in people_list:
#             if isinstance(p, dict):
#                 pname = str(p.get("person_name", "")).strip()
#                 ptitle = str(p.get("job_title", "")).strip()
#                 if pname and pname.lower() not in ("null", "none", "n/a", "unknown", "not found"):
#                     names.append(pname)
#                 if ptitle and ptitle.lower() not in ("null", "none", "n/a", "unknown", "not found"):
#                     titles.append(ptitle)
#         result["person_name"] = ", ".join(names) if names else "Not Found"
#         result["job_title"] = ", ".join(titles) if titles else "Not Found"
#     else:
#         # Fallback to old flat format (backward compatibility)
#         for key in ("person_name", "job_title"):
#             val = str(data.get(key, "")).strip()
#             if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#                 result[key] = "Not Found"
#             else:
#                 result[key] = val

#     for key in ("service", "sales_hook"):
#         val = str(data.get(key, "")).strip()
#         if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#             result[key] = "Not Found"
#         else:
#             result[key] = val

#     rel = str(data.get("relevancy", "")).strip().lower()
#     result["relevancy"] = "True" if rel in ("true", "yes", "1", "high") else "False"

#     if not AI_REQUIRED_KEYS.issubset(result.keys()):
#         return None

#     return result


# def _ai_get_provider_class(name: str):
#     return getattr(Provider, name, None)


# def _ai_pick_model(provider_class) -> str:
#     models = getattr(provider_class, "models", None)
#     return models[0] if models else AI_FALLBACK_MODEL


# def _ai_call_provider(provider_name: str, prompt: str) -> Optional[Dict[str, Any]]:
#     provider_class = _ai_get_provider_class(provider_name)
#     if provider_class is None:
#         return None
#     model = _ai_pick_model(provider_class)
#     client = Client()
#     try:
#         response = client.chat.completions.create(
#             model=model,
#             provider=provider_class,
#             messages=[{"role": "user", "content": prompt}],
#             stream=False,
#             timeout=AI_PER_CALL_TIMEOUT,
#         )
#         content = None
#         if getattr(response, "choices", None):
#             message = response.choices[0].message
#             content = getattr(message, "content", None)
#         if not isinstance(content, str):
#             return None
#         return _ai_parse_json(content)
#     except Exception:
#         return None
#     finally:
#         del client


# def _ai_race_providers(prompt: str) -> Optional[Dict[str, Any]]:
#     with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as executor:
#         future_to_name = {
#             executor.submit(_ai_call_provider, name, prompt): name
#             for name in AI_PROVIDER_NAMES
#         }
#         winner = None
#         try:
#             for future in concurrent.futures.as_completed(future_to_name, timeout=AI_RACE_TIMEOUT):
#                 try:
#                     result = future.result()
#                 except Exception:
#                     result = None
#                 if result is not None:
#                     winner = result
#                     break
#         except concurrent.futures.TimeoutError:
#             pass
#         executor.shutdown(wait=False, cancel_futures=True)
#     return winner


# def ai_get_lead_info(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> Dict[str, str]:
#     if not AI_ENABLED:
#         return {
#             "person_name": "Not Found", "job_title": "Not Found",
#             "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#         }

#     prompt = _ai_build_prompt(domain, company_name, leadership_text, general_text, target)

#     for attempt in range(1, AI_MAX_RETRIES + 1):
#         result = _ai_race_providers(prompt)
#         if result is not None:
#             return result
#         delay = AI_RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
#         time.sleep(min(delay, 20))
#         gc.collect()

#     return {
#         "person_name": "Not Found", "job_title": "Not Found",
#         "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#     }


# # ============================================================
# # SPIDER
# # ============================================================

# class FinderSpider(scrapy.Spider):
#     name = "Gliner_Spider"

#     EMAIL_REGEX = re.compile(
#         r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}",
#         re.IGNORECASE,
#     )
#     MAILTO_REGEX = re.compile(
#         r"mailto:\s*([A-Za-z0-9._%+\-]+(?:%40|@)[A-Za-z0-9.\-]+\.[A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     OBFUSCATED_EMAIL_REGEX = re.compile(
#         r"([A-Za-z0-9._%+\-]{1,64})\s*"
#         r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\}|\s+at\s+|@)\s*"
#         r"([A-Za-z0-9.\-]{1,253})\s*"
#         r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\s+dot\s+|\.)\s*"
#         r"([A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     HEX_ESCAPE_REGEX = re.compile(r"\\x([0-9a-fA-F]{2})")
#     UNICODE_ESCAPE_REGEX = re.compile(r"\\u([0-9a-fA-F]{4})")

#     # ═══════════════════════════════════════════════════════════
#     # NEW: deterministic social & phone extraction patterns
#     # ═══════════════════════════════════════════════════════════
#     TEL_HREF_REGEX = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)

#     SOCIAL_DOMAINS = {
#         "twitter": ["twitter.com", "x.com"],
#         "linkedin": ["linkedin.com"],
#         "facebook": ["facebook.com", "fb.com"],
#         "instagram": ["instagram.com"],
#         "youtube": ["youtube.com", "youtu.be"],
#     }
#     OTHER_SOCIAL_DOMAINS = [
#         "tiktok.com", "pinterest.com", "reddit.com", "github.com",
#         "medium.com", "snapchat.com", "discord.com", "discord.gg",
#         "t.me", "telegram.me", "whatsapp.com", "wa.me",
#     ]

#     IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico")
#     TRASH_EMAIL_DOMAINS = {
#         "sentry.io",
#         "example.com",
#         "example.org",
#         "example.net",
#         "mysite.com",
#         "yourdomain.com",
#         "domain.com",
#     }
#     TRASH_EMAIL_LOCAL_PARTS = {
#         "example", "test", "email", "user", "demo", "sample",
#         "dummy", "null", "none", "noreply", "no-reply",
#         "donotreply", "do-not-reply",
#         ".png", ".svg", ".jpg", ".jpeg", ".webp", ".gif",
#     }

#     custom_settings = {
#         "DEPTH_LIMIT": 10,
#         "CONCURRENT_REQUESTS": 16,
#         "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
#         "DOWNLOAD_TIMEOUT": 15,
#         "RETRY_ENABLED": False,
#         "ITEM_PIPELINES": {
#             "Nizami.pipelines.QualifiedSitesCsvPipeline": 300,
#         },
#         "QUALIFIED_SITES_OUTPUT": "output.csv",
#         "BRUTE_EMAIL_VALIDATE_DNS": False,
#     }

#     FOLLOW_URL_PATTERNS = [
#         "team", "our-team", "leadership", "executive", "founder",
#         "about", "about-us", "who-we-are", "management", "directors",
#         "people", "staff", "board", "c-suite",
#         "contact", "contact-us", "get-in-touch",
#         "company", "partners", "locations",
#         "support", "info", "email", "address",
#         "write-to-us", "inquiry", "enquiry", "hello"
#     ]

#     LEADERSHIP_PATTERNS = [
#         "team", "our-team", "leadership", "executive", "founder",
#         "about", "about-us", "who-we-are", "management", "directors",
#         "people", "staff", "board", "c-suite"
#     ]

#     LEADERSHIP_SEED_PATHS = [
#         "about", "team", "leadership", "about-us", "meet-our-team",
#         "who-we-are", "management", "our-team", "executive-team",
#         "company/team", "company/about", "people", "staff",
#         "directors", "board", "founders", "company/leadership",
#     ]

#     AI_PAGE_TEXT_CAP = AI_MAX_TEXT_CHARS

#     @classmethod
#     def from_crawler(cls, crawler, *args, **kwargs):
#         spider = super().from_crawler(crawler, *args, **kwargs)
#         spider.crawler = crawler
#         crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
#         crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
#         return spider

#     def spider_closed(self, spider):
#         gc.collect()

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self.domain_phones = {}
#         self.domain_companies = {}
#         self.domain_websites = {}
#         self.domain_originals = {}
#         self.email_domain_dns_cache = {}
#         self.dns_resolver = dns.resolver.Resolver()
#         self.dns_resolver.timeout = 1.0
#         self.dns_resolver.lifetime = 2.0

#         self.include_keywords, self.block_keywords = self.load_keywords()
#         self.start_urls_list = self.load_input_files()

#         self.visited = set()
#         self.domain_data = {}
#         self.yielded_domains = set()
#         self.enriched_domains = set()

#         self.batch_size = 10
#         self.all_urls = self.start_urls_list
#         self.current_batch = -1

#         self.target = kwargs.get("target", "")
#         if not self.target:
#             self.target = getattr(self, "target", "")

#         if AI_ENABLED:
#             self.logger.info("AI ENABLED (%s providers).", len(AI_PROVIDER_NAMES))
#             if self.target:
#                 self.logger.info("Target: '%s'", self.target)
#         else:
#             self.logger.warning("g4f not installed -- AI DISABLED.")

#     def _start_batch(self, batch_index):
#         start = batch_index * self.batch_size
#         end = min(start + self.batch_size, len(self.all_urls))
#         batch_urls = self.all_urls[start:end]
#         if not batch_urls:
#             return

#         self.current_batch = batch_index
#         self.logger.info(
#             "Starting batch %s with %s URLs",
#             batch_index + 1,
#             len(batch_urls),
#         )

#         for url in batch_urls:
#             yield scrapy.Request(
#                 url=url,
#                 callback=self.parse_page,
#                 errback=self.handle_request_error,
#                 meta={"depth": 0, "root_url": url, "batch_index": batch_index},
#             )

#     async def start(self):
#         for request in self.start_requests():
#             yield request

#     def start_requests(self):
#         yield from self._start_batch(0)

#     def spider_idle(self, spider):
#         next_batch = self.current_batch + 1
#         if next_batch * self.batch_size >= len(self.all_urls):
#             return

#         self.logger.info("Spider idle, starting next batch %s", next_batch + 1)
#         for request in self._start_batch(next_batch):
#             try:
#                 self.crawler.engine.crawl(request)
#             except TypeError:
#                 self.crawler.engine.crawl(request, spider)

#         raise DontCloseSpider

#     @staticmethod
#     def normalize_domain(url):
#         return urlparse(url).netloc.lower().replace("www.", "").strip()

#     @staticmethod
#     def normalize_url(raw_url):
#         if pd.isna(raw_url):
#             return None
#         url = str(raw_url).strip()
#         if not url:
#             return None
#         if not url.startswith(("http://", "https://")):
#             url = f"http://{url}"
#         parsed = urlparse(url)
#         if not parsed.netloc or " " in parsed.netloc:
#             return None
#         return url

#     @classmethod
#     def is_trash_email(cls, email):
#         value = email.strip().lower()
#         if "@" not in value:
#             return True
#         local, domain = value.rsplit("@", 1)
#         domain = domain.lstrip(".")
#         if domain.startswith("www."):
#             domain = domain[4:]
#         if not local or not domain:
#             return True
#         if domain in cls.TRASH_EMAIL_DOMAINS or domain.startswith("example."):
#             return True
#         if domain.endswith(".sentry.io") or "mysite" in domain:
#             return True
#         if domain.endswith("wixpress.com"):
#             return True
#         if local in cls.TRASH_EMAIL_LOCAL_PARTS:
#             return True
#         if re.fullmatch(r"[0-9a-f]{24,}", local):
#             return True
#         if re.fullmatch(r"[0-9a-f-]{30,}", local):
#             return True
#         tld = domain.rsplit(".", 1)[-1]
#         if not re.fullmatch(r"[a-z]{2,10}", tld):
#             return True
#         if value.endswith(cls.IMAGE_SUFFIXES):
#             return True
#         return False

#     def email_domain_has_dns(self, domain):
#         key = domain.strip().lower()
#         if not key:
#             return False
#         cached = self.email_domain_dns_cache.get(key)
#         if cached is not None:
#             return cached
#         is_valid = False
#         try:
#             mx_records = self.dns_resolver.resolve(key, "MX")
#             is_valid = bool(mx_records)
#         except Exception:
#             try:
#                 a_records = self.dns_resolver.resolve(key, "A")
#                 is_valid = bool(a_records)
#             except Exception:
#                 is_valid = False
#         self.email_domain_dns_cache[key] = is_valid
#         return is_valid

#     def load_keywords(self):
#         project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
#         keyword_path = None
#         for file in os.listdir(project_root):
#             if file.lower() == "keywords.json":
#                 keyword_path = os.path.join(project_root, file)
#                 break
#         if not keyword_path:
#             raise FileNotFoundError(
#                 f"Keywords.json file not found in project root: {project_root}"
#             )
#         with open(keyword_path, "r", encoding="utf-8") as file:
#             data = json.load(file)
#         include_raw = data.get("include_keywords", [])
#         self.keyword_category = {}
#         if isinstance(include_raw, dict):
#             include = {}
#             for category, keywords in include_raw.items():
#                 category_keywords = [kw.lower() for kw in (keywords or [])]
#                 include[category] = category_keywords
#                 for kw in category_keywords:
#                     self.keyword_category[kw] = category
#         else:
#             include = {"General": [kw.lower() for kw in include_raw]}
#             self.keyword_category = {kw: "General" for kw in include["General"]}
#         block = [kw.lower() for kw in data.get("block_keywords", [])]
#         return include, block

#     def load_input_files(self):
#         input_folder = os.path.join(
#             os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Input"
#         )
#         urls = []
#         for file in sorted(os.listdir(input_folder)):
#             path = os.path.join(input_folder, file)
#             try:
#                 if file.endswith(".csv"):
#                     df = pd.read_csv(path, sep=None, engine="python", on_bad_lines="warn")
#                 elif file.endswith(".xlsx") or file.endswith(".xls"):
#                     df = pd.read_excel(path)
#                 else:
#                     continue
#             except Exception as error:
#                 self.logger.warning("Failed to read %s: %s", file, error)
#                 continue

#             url_column = None
#             phone_column = None
#             company_column = None
#             for col in df.columns:
#                 col_name = str(col).strip().lower()
#                 if col_name in ["website", "url", "website url"] and url_column is None:
#                     url_column = col
#                 if ("phone" in col_name or "contact" in col_name) and phone_column is None:
#                     phone_column = col
#                 if col_name in ["company name", "agency name", "name", "business name"] and company_column is None:
#                     company_column = col

#             if url_column is None:
#                 self.logger.warning("No URL column found in %s", file)
#                 continue

#             for _, row in df.iterrows():
#                 normalized_url = self.normalize_url(row[url_column])
#                 if not normalized_url:
#                     continue
#                 urls.append(normalized_url)
#                 domain = self.normalize_domain(normalized_url)

#                 if domain not in self.domain_originals:
#                     self.domain_originals[domain] = {}
#                     for col in df.columns:
#                         val = row.get(col)
#                         if pd.notna(val):
#                             self.domain_originals[domain][col] = str(val).strip()

#                 if domain not in self.domain_websites:
#                     self.domain_websites[domain] = normalized_url
#                 if phone_column is not None:
#                     phone = row[phone_column]
#                     if pd.notna(phone):
#                         phone_value = str(phone).strip()
#                         if phone_value and phone_value.lower() != "nan":
#                             self.domain_phones[domain] = phone_value
#                 if company_column is not None and domain not in self.domain_companies:
#                     company = row[company_column]
#                     if pd.notna(company):
#                         company_value = str(company).strip()
#                         if company_value and company_value.lower() != "nan":
#                             self.domain_companies[domain] = company_value
#         return list(set(urls))

#     @classmethod
#     def _decode_js_escapes(cls, source_text):
#         if not source_text:
#             return ""
#         decoded = cls.HEX_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), source_text,
#         )
#         decoded = cls.UNICODE_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), decoded,
#         )
#         return decoded

#     @classmethod
#     def _extract_emails_from_source(cls, source_text):
#         if not source_text:
#             return set()
#         blobs = []
#         raw = source_text
#         html_unescaped = unescape(raw)
#         url_decoded = unquote(html_unescaped)
#         js_decoded = cls._decode_js_escapes(url_decoded)
#         for blob in (raw, html_unescaped, url_decoded, js_decoded):
#             if blob and blob not in blobs:
#                 blobs.append(blob)
#         found = set()
#         for blob in blobs:
#             for email in cls.EMAIL_REGEX.findall(blob):
#                 found.add(email.strip().lower())
#             for email in cls.MAILTO_REGEX.findall(blob):
#                 normalized = unquote(email).strip().lower().replace("%40", "@")
#                 found.add(normalized)
#             for match in cls.OBFUSCATED_EMAIL_REGEX.finditer(blob):
#                 local = match.group(1).strip().lower()
#                 domain_part = match.group(2).strip().lower().strip(".")
#                 tld = match.group(3).strip().lower()
#                 found.add(f"{local}@{domain_part}.{tld}")
#         return found

#     def _build_email_candidates(self, source_text):
#         candidates = []
#         seen = set()
#         extracted = self._extract_emails_from_source(source_text)
#         for email in extracted:
#             normalized_email = email.strip().lower().replace("mailto:", "").strip(" <>\"'(),;")
#             if not normalized_email or normalized_email in seen:
#                 continue
#             if self.is_trash_email(normalized_email):
#                 continue
#             seen.add(normalized_email)
#             candidates.append(normalized_email)
#         return candidates

#     # ═══════════════════════════════════════════════════════════
#     # NEW: deterministic social-link & phone extraction
#     # ═══════════════════════════════════════════════════════════
#     def _extract_socials(self, response, domain):
#         """Scrape every social link from the page and bucket by platform."""
#         data = self.domain_data[domain]
#         for link in response.css("a::attr(href)").getall():
#             full_url = response.urljoin(link)
#             parsed = urlparse(full_url)
#             netloc = parsed.netloc.lower().replace("www.", "")

#             # Skip empty / mailto / tel
#             if not netloc or netloc.startswith("mailto") or netloc.startswith("tel"):
#                 continue

#             matched = False
#             for platform, domains in self.SOCIAL_DOMAINS.items():
#                 if any(d in netloc for d in domains):
#                     data["socials"][platform].add(full_url)
#                     matched = True
#                     break

#             if not matched:
#                 if any(d in netloc for d in self.OTHER_SOCIAL_DOMAINS):
#                     data["socials"]["other"].add(full_url)

#     def _extract_tel_phones(self, source_text, domain):
#         """Scrape phone numbers from href='tel:...' links."""
#         data = self.domain_data[domain]
#         for match in self.TEL_HREF_REGEX.findall(source_text):
#             raw = match.strip()
#             # Clean: remove visual separators but keep + and digits
#             cleaned = re.sub(r"[^\d+]", "", raw)
#             if cleaned:
#                 data["phones_scraped"].add(cleaned)

#     def handle_request_error(self, failure):
#         request = failure.request
#         self.logger.warning(
#             "Request failed: %s (%s)",
#             request.url, failure.value,
#         )




# import os
# os.environ["SE_HEADLESS"] = "1"
# os.environ["PLAYWRIGHT_HEADLESS"] = "1"
# os.environ["CHROME_HEADLESS"] = "1"
# os.environ["MOZ_HEADLESS"] = "1"
# os.environ["PYPPETEER_HEADLESS"] = "1"
# os.environ["HEADLESS"] = "1"
# import gc
# import json
# import random
# import re
# import time
# import warnings
# import concurrent.futures
# from html import unescape
# from typing import Any, Dict, List, Optional, Tuple
# from urllib.parse import unquote, urlparse, urljoin
# from xml.etree import ElementTree as ET

# import dns.resolver
# import pandas as pd
# import scrapy
# from scrapy import signals
# from scrapy.exceptions import DontCloseSpider
# from scrapy.utils.defer import maybe_deferred_to_future
# from twisted.internet import threads
# from twisted.internet.defer import DeferredList

# try:
#     import g4f
#     from g4f import Provider
#     from g4f.client import Client
#     HAS_G4F = True
# except ImportError:
#     HAS_G4F = False

# try:
#     from bs4 import BeautifulSoup
#     HAS_BS4 = True
# except ImportError:
#     HAS_BS4 = False

# try:
#     import fasttext
#     HAS_FASTTEXT = True
# except ImportError:
#     HAS_FASTTEXT = False

# warnings.filterwarnings("ignore", message="Unclosed client session")
# warnings.filterwarnings("ignore", message="Unclosed connector")

# AI_ENABLED = HAS_G4F
# AI_PROVIDER_NAMES = [
#     "Cloudflare",
#     "CohereForAI_C4AI_Command",
#     "AnyProvider",
#     "Gemini",
#     "Perplexity",
#     "Yqcloud",
#     "OpenaiChat",
#     "MiniMax",
#     "WhiteRabbitNeo",
# ]
# AI_FALLBACK_MODEL = "gpt-4o-mini"
# AI_PER_CALL_TIMEOUT = 35
# AI_RACE_TIMEOUT = 50
# AI_MAX_RETRIES = 5
# AI_RETRY_BASE_DELAY = 2.0
# AI_MAX_WORKERS = min(12, len(AI_PROVIDER_NAMES))
# AI_MAX_TEXT_CHARS = 15000
# AI_CHUNK_MIN_LENGTH = 50
# AI_CHUNK_MAX_LENGTH = 1200
# AI_CODENL_THRESHOLD = 0.85
# AI_REQUIRED_KEYS = {"person_name", "job_title", "service", "relevancy", "sales_hook"}

# class CodeNLFilter:
#     def __init__(self, model_path: str, threshold: float = AI_CODENL_THRESHOLD):
#         self.model_path = model_path
#         self.threshold = threshold
#         self.model: Optional[fasttext.FastText] = None
#         self._load_model()

#     def _load_model(self) -> None:
#         if not HAS_FASTTEXT:
#             return
#         if not os.path.exists(self.model_path):
#             return
#         try:
#             self.model = fasttext.load_model(self.model_path)
#         except Exception:
#             self.model = None

#     def is_code(self, text: str) -> Tuple[bool, float]:
#         if self.model is None or not text or len(text) < 10:
#             return False, 0.0
#         try:
#             labels, probs = self.model.predict(text.replace("\n", " "), k=1)
#             label = labels[0].replace("__label__", "")
#             confidence = float(probs[0])
#             is_code = (label.lower() == "code" and confidence >= self.threshold)
#             return is_code, confidence
#         except Exception:
#             return False, 0.0

#     def filter_text(self, text: str) -> str:
#         if self.model is None or not text:
#             return text
#         chunks = self._semantic_chunk(text)
#         clean_chunks: List[str] = []
#         for chunk in chunks:
#             is_code, _ = self.is_code(chunk)
#             if not is_code:
#                 clean_chunks.append(chunk)
#         return "\n".join(clean_chunks)

#     @staticmethod
#     def _semantic_chunk(text: str) -> List[str]:
#         paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
#         chunks: List[str] = []
#         for para in paragraphs:
#             if len(para) < AI_CHUNK_MIN_LENGTH:
#                 continue
#             if len(para) <= AI_CHUNK_MAX_LENGTH:
#                 chunks.append(para)
#             else:
#                 sentences = re.split(r'(?<=[.!?])\s+', para)
#                 current = ""
#                 for sent in sentences:
#                     sent = sent.strip()
#                     if not sent:
#                         continue
#                     if len(current) + len(sent) + 1 <= AI_CHUNK_MAX_LENGTH:
#                         current = (current + " " + sent).strip() if current else sent
#                     else:
#                         if current and len(current) >= AI_CHUNK_MIN_LENGTH:
#                             chunks.append(current)
#                         current = sent
#                 if current and len(current) >= AI_CHUNK_MIN_LENGTH:
#                     chunks.append(current)
#         return chunks

# _code_nl_filter: Optional[CodeNLFilter] = None

# def _get_code_nl_filter() -> Optional[CodeNLFilter]:
#     global _code_nl_filter
#     if _code_nl_filter is not None:
#         return _code_nl_filter
#     if not HAS_FASTTEXT:
#         return None
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     model_path = os.path.join(script_dir, "code_nl_model.bin")
#     _code_nl_filter = CodeNLFilter(model_path, AI_CODENL_THRESHOLD)
#     return _code_nl_filter

# # ============================================================
# # FIX 1: Updated _ai_clean_text (old version commented out)
# # ============================================================
# # (Old version is commented inside the first big block; the active version is below)
# def _ai_clean_text(html_text: str) -> str:
#     if not html_text:
#         return ""
#     if HAS_BS4:
#         soup = BeautifulSoup(html_text, "lxml")
#         for tag in soup(["script", "style", "noscript", "svg", "iframe", "nav", "footer"]):
#             tag.decompose()
#         for tag in soup.find_all(True):
#             if not hasattr(tag, "name") or tag.name is None:
#                 continue
#             if getattr(tag, "attrs", None) is None:
#                 continue
#             try:
#                 cls = " ".join(tag.get("class") or []).lower()
#                 id_attr = (tag.get("id") or "").lower()
#             except Exception:
#                 continue
#             if any(kw in cls or kw in id_attr for kw in [
#                 "testimonial", "case-study", "case_study", "review", "quote",
#                 "customer-story", "success-story", "client-logo", "partner-logo",
#                 "client-list", "partner-list", "trusted-by", "used-by",
#             ]):
#                 tag.decompose()
#         text = soup.get_text(separator=" ", strip=True)
#     else:
#         text = re.sub(r"<[^>]+>", " ", html_text)
#     text = re.sub(r"\s+", " ", text).strip()
#     code_nl = _get_code_nl_filter()
#     if code_nl is not None and code_nl.model is not None:
#         text = code_nl.filter_text(text)
#     return text

# # ============================================================
# # FIX 2: Updated _ai_build_prompt (old version commented out)
# # ============================================================
# # OLD _ai_build_prompt (commented out)
# """
# def _ai_build_prompt(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> str:
#     target_str = target if target else "general business relevance"
#     example = (...)
#     parts = [...]
#     return "\n".join(parts)
# """
# def _ai_build_prompt(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> str:
#     target_str = target if target else "general business relevance"
#     example = (
#         '{"people":['
#         '{"person_name":"Alex Smith","job_title":"Founder & CEO","context":"Alex Smith, Founder & CEO, leads our team"},'
#         '{"person_name":"Max Doe","job_title":"CTO","context":"Max Doe serves as CTO"}'
#         '],"service":"AI recruitment software","relevancy":true,'
#         '"sales_hook":"What is the biggest bottleneck in scaling your AI recruitment platform?"}'
#     )
#     parts = [
#         "You are a strict B2B lead researcher. Extract ONLY current employees of THIS company.",
#         "",
#         "=== ABSOLUTE RULES — VIOLATING ANY OF THESE IS A FAILURE ===",
#         "1. people: JSON array of objects. Each object MUST have:",
#         "   - person_name: FULL NAME only. No company names, no quotes, no descriptors.",
#         "   - job_title: Their EXACT internal job title at THIS company ONLY.",
#         "   - context: The EXACT sentence/phrase from the text where this person appears with their title.",
#         "",
#         "2. REJECT and NEVER extract any of the following:",
#         "   - Customer testimonial givers (e.g. 'Jane Doe, CMO at ClientCorp')",
#         "   - Case study subjects or featured clients",
#         "   - Partner company employees or partner logos",
#         "   - Advisory board members, board of directors, or external advisors UNLESS they are full-time employees",
#         "   - Investors, VCs, or funding partners",
#         "   - Former employees, ex-employees, or alumni",
#         "   - Anyone whose title mentions another company name (e.g. 'CMO, Vercel' or 'Manager at Zendesk')",
#         "   - Anyone appearing in a quote, review, or testimonial section",
#         "   - Generic placeholders like 'John Doe' or 'Jane Smith'",
#         "",
#         "3. A person MUST be a current internal employee of the company being scraped.",
#         "   - Titles like 'CMO, Vercel' or 'Director at Google' = EXTERNAL → REJECT",
#         "   - Titles like 'Our client, Bob from Acme' = EXTERNAL → REJECT",
#         "   - Titles like 'Former CTO' or 'Ex-CEO' = EXTERNAL → REJECT",
#         "   - Titles with '@' and another company name (e.g. 'CEO @ MealBox') = EXTERNAL → REJECT",
#         "",
#         "4. If you are unsure whether a person is an employee, REJECT them.",
#         "",
#         "5. service: What this company sells/does in 2-8 words. Be specific.",
#         "",
#         '6. relevancy: true or false. Does this company match: "' + target_str + '"?',
#         "   - true = clearly fits. false = does not fit or unclear.",
#         "",
#         "7. sales_hook: ONE concise first question (max 12 words) for the MOST SENIOR employee.",
#         "",
#         "Respond with ONLY a single-line valid JSON object. No markdown, no explanation.",
#         "",
#         "Example: " + example,
#         "",
#         "=== LEADERSHIP / TEAM PAGES (highest priority — extract from these first) ===",
#         '"""' + (leadership_text[:8000] if leadership_text else "(no leadership text scraped)") + '"""',
#         "",
#         "=== GENERAL SITE PAGES (lower priority — use only if leadership pages are empty) ===",
#         '"""' + (general_text[:6000] if general_text else "(no general text scraped)") + '"""',
#     ]
#     return "\n".join(parts)

# # ============================================================
# # FIX 3: Updated _ai_parse_json (old version commented out)
# # ============================================================
# # (Active version already has the post‑validation; we keep it as is)
# def _ai_parse_json(raw: str) -> Optional[Dict[str, Any]]:
#     if not raw:
#         return None
#     raw = raw.strip()
#     raw = re.sub(r"^```(json)?", "", raw, flags=re.IGNORECASE).strip()
#     raw = re.sub(r"```$", "", raw).strip()
#     match = re.search(r"\{.*\}", raw, re.DOTALL)
#     if not match:
#         return None
#     try:
#         data = json.loads(match.group(0))
#     except json.JSONDecodeError:
#         return None

#     if not isinstance(data, dict):
#         return None

#     REJECT_TITLE_KEYWORDS = {
#         " at ", " of ", " from ", "formerly", "ex-", "ex ", "former ", "client",
#         "customer", "partner", "partnered", "testimonial", "advisory", "investor",
#         "board member", "external", "alumni", "retired",
#     }
#     REJECT_CONTEXT_KEYWORDS = {
#         "testimonial", "client", "customer", "partner", "partnered with",
#         "case study", "our client", "our customer", "success story",
#         "featured", "review", "quote from", "says ", "said ", "trusted by",
#     }
#     PLACEHOLDER_NAMES = {"john doe", "jane smith", "jane doe", "john smith", "placeholder"}

#     result = {}
#     people_list = data.get("people", [])
#     if isinstance(people_list, list) and people_list:
#         names = []
#         titles = []
#         for p in people_list:
#             if not isinstance(p, dict):
#                 continue
#             pname = str(p.get("person_name", "")).strip()
#             ptitle = str(p.get("job_title", "")).strip()
#             pcontext = str(p.get("context", "")).strip().lower()
#             if not pname or not ptitle:
#                 continue
#             if pname.lower() in PLACEHOLDER_NAMES:
#                 continue
#             if ptitle.lower() in ("null", "none", "n/a", "unknown", "not found"):
#                 continue
#             title_lower = ptitle.lower()
#             if any(kw in title_lower for kw in REJECT_TITLE_KEYWORDS):
#                 continue
#             if any(kw in pcontext for kw in REJECT_CONTEXT_KEYWORDS):
#                 continue
#             if re.search(r",\s*[A-Z][a-zA-Z]+", ptitle):
#                 continue
#             if re.search(r"@\s*[A-Z]", ptitle):
#                 continue
#             names.append(pname)
#             titles.append(ptitle)
#         result["person_name"] = ", ".join(names) if names else "Not Found"
#         result["job_title"] = ", ".join(titles) if titles else "Not Found"
#     else:
#         for key in ("person_name", "job_title"):
#             val = str(data.get(key, "")).strip()
#             if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#                 result[key] = "Not Found"
#             else:
#                 result[key] = val

#     for key in ("service", "sales_hook"):
#         val = str(data.get(key, "")).strip()
#         if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#             result[key] = "Not Found"
#         else:
#             result[key] = val

#     rel = str(data.get("relevancy", "")).strip().lower()
#     result["relevancy"] = "True" if rel in ("true", "yes", "1", "high") else "False"

#     if not AI_REQUIRED_KEYS.issubset(result.keys()):
#         return None

#     return result

# def _ai_get_provider_class(name: str):
#     return getattr(Provider, name, None)

# def _ai_pick_model(provider_class) -> str:
#     models = getattr(provider_class, "models", None)
#     return models[0] if models else AI_FALLBACK_MODEL

# def _ai_call_provider(provider_name: str, prompt: str) -> Optional[Dict[str, Any]]:
#     provider_class = _ai_get_provider_class(provider_name)
#     if provider_class is None:
#         return None
#     model = _ai_pick_model(provider_class)
#     client = Client()
#     try:
#         response = client.chat.completions.create(
#             model=model,
#             provider=provider_class,
#             messages=[{"role": "user", "content": prompt}],
#             stream=False,
#             timeout=AI_PER_CALL_TIMEOUT,
#         )
#         content = None
#         if getattr(response, "choices", None):
#             message = response.choices[0].message
#             content = getattr(message, "content", None)
#         if not isinstance(content, str):
#             return None
#         return _ai_parse_json(content)
#     except Exception:
#         return None
#     finally:
#         del client

# def _ai_race_providers(prompt: str) -> Optional[Dict[str, Any]]:
#     with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as executor:
#         future_to_name = {
#             executor.submit(_ai_call_provider, name, prompt): name
#             for name in AI_PROVIDER_NAMES
#         }
#         winner = None
#         try:
#             for future in concurrent.futures.as_completed(future_to_name, timeout=AI_RACE_TIMEOUT):
#                 try:
#                     result = future.result()
#                 except Exception:
#                     result = None
#                 if result is not None:
#                     winner = result
#                     break
#         except concurrent.futures.TimeoutError:
#             pass
#         executor.shutdown(wait=False, cancel_futures=True)
#     return winner

# def ai_get_lead_info(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> Dict[str, str]:
#     if not AI_ENABLED:
#         return {
#             "person_name": "Not Found", "job_title": "Not Found",
#             "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#         }
#     prompt = _ai_build_prompt(domain, company_name, leadership_text, general_text, target)
#     for attempt in range(1, AI_MAX_RETRIES + 1):
#         result = _ai_race_providers(prompt)
#         if result is not None:
#             return result
#         delay = AI_RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
#         time.sleep(min(delay, 20))
#         gc.collect()
#     return {
#         "person_name": "Not Found", "job_title": "Not Found",
#         "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#     }

# # ============================================================
# # SPIDER
# # ============================================================

# class FinderSpider(scrapy.Spider):
#     name = "Gliner_Spider"

#     EMAIL_REGEX = re.compile(
#         r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}",
#         re.IGNORECASE,
#     )
#     MAILTO_REGEX = re.compile(
#         r"mailto:\s*([A-Za-z0-9._%+\-]+(?:%40|@)[A-Za-z0-9.\-]+\.[A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     OBFUSCATED_EMAIL_REGEX = re.compile(
#         r"([A-Za-z0-9._%+\-]{1,64})\s*"
#         r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\}|\s+at\s+|@)\s*"
#         r"([A-Za-z0-9.\-]{1,253})\s*"
#         r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\s+dot\s+|\.)\s*"
#         r"([A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     HEX_ESCAPE_REGEX = re.compile(r"\\x([0-9a-fA-F]{2})")
#     UNICODE_ESCAPE_REGEX = re.compile(r"\\u([0-9a-fA-F]{4})")
#     TEL_HREF_REGEX = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)

#     SOCIAL_DOMAINS = {
#         "twitter": ["twitter.com", "x.com"],
#         "linkedin": ["linkedin.com"],
#         "facebook": ["facebook.com", "fb.com"],
#         "instagram": ["instagram.com"],
#         "youtube": ["youtube.com", "youtu.be"],
#     }
#     OTHER_SOCIAL_DOMAINS = [
#         "tiktok.com", "pinterest.com", "reddit.com", "github.com",
#         "medium.com", "snapchat.com", "discord.com", "discord.gg",
#         "t.me", "telegram.me", "whatsapp.com", "wa.me",
#     ]

#     IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico")
#     TRASH_EMAIL_DOMAINS = {
#         "sentry.io",
#         "example.com",
#         "example.org",
#         "example.net",
#         "mysite.com",
#         "yourdomain.com",
#         "domain.com",
#     }
#     TRASH_EMAIL_LOCAL_PARTS = {
#         "example", "test", "email", "user", "demo", "sample",
#         "dummy", "null", "none", "noreply", "no-reply",
#         "donotreply", "do-not-reply",
#         ".png", ".svg", ".jpg", ".jpeg", ".webp", ".gif",
#     }

#     custom_settings = {
#         "DEPTH_LIMIT": 10,
#         "CONCURRENT_REQUESTS": 16,
#         "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
#         "DOWNLOAD_TIMEOUT": 15,
#         "RETRY_ENABLED": False,
#         "ITEM_PIPELINES": {
#             "Nizami.pipelines.QualifiedSitesCsvPipeline": 300,
#         },
#         "QUALIFIED_SITES_OUTPUT": "output.csv",
#         "BRUTE_EMAIL_VALIDATE_DNS": False,
#     }

#     FOLLOW_URL_PATTERNS = [
#         "team", "our-team", "leadership", "executive", "founder",
#         "about", "about-us", "who-we-are", "management", "directors",
#         "people", "staff", "board", "c-suite",
#         "contact", "contact-us", "get-in-touch",
#         "company", "partners", "locations",
#         "support", "info", "email", "address",
#         "write-to-us", "inquiry", "enquiry", "hello"
#     ]

#     LEADERSHIP_PATTERNS = [
#         "team", "our-team", "leadership", "executive", "founder",
#         "about", "about-us", "who-we-are", "management", "directors",
#         "people", "staff", "board", "c-suite"
#     ]

#     LEADERSHIP_SEED_PATHS = [
#         "about", "team", "leadership", "about-us", "meet-our-team",
#         "who-we-are", "management", "our-team", "executive-team",
#         "company/team", "company/about", "people", "staff",
#         "directors", "board", "founders", "company/leadership",
#     ]

#     # ────────────────────────────────────────────────────────────────
#     # NEW class attributes (added per instructions)
#     # ────────────────────────────────────────────────────────────────
#     LEADERSHIP_PRIORITY = {
#         "team": 100, "our-team": 100, "leadership": 100,
#         "executive": 95, "founder": 95, "about": 80,
#         "about-us": 80, "who-we-are": 80, "people": 80,
#         "staff": 70, "directors": 70, "board": 70,
#         "c-suite": 70, "management": 70,
#     }

#     LEADERSHIP_TEXT_SIGNALS = [
#         "our leadership", "meet our team", "executive team",
#         "management team", "our people", "board of directors",
#         "leadership team", "founders", "the team", "team members",
#         "leadership", "executives", "directors", "management",
#     ]

#     AI_PAGE_TEXT_CAP = AI_MAX_TEXT_CHARS

#     @classmethod
#     def from_crawler(cls, crawler, *args, **kwargs):
#         spider = super().from_crawler(crawler, *args, **kwargs)
#         spider.crawler = crawler
#         crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
#         crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
#         return spider

#     def spider_closed(self, spider):
#         gc.collect()

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self.domain_phones = {}
#         self.domain_companies = {}
#         self.domain_websites = {}
#         self.domain_originals = {}
#         self.email_domain_dns_cache = {}
#         self.dns_resolver = dns.resolver.Resolver()
#         self.dns_resolver.timeout = 1.0
#         self.dns_resolver.lifetime = 2.0

#         self.include_keywords, self.block_keywords = self.load_keywords()
#         self.start_urls_list = self.load_input_files()

#         self.visited = set()
#         self.domain_data = {}
#         self.yielded_domains = set()
#         self.enriched_domains = set()

#         self.batch_size = 10
#         self.all_urls = self.start_urls_list
#         self.current_batch = -1

#         self.target = kwargs.get("target", "")
#         if not self.target:
#             self.target = getattr(self, "target", "")

#         code_nl = _get_code_nl_filter()
#         if code_nl is not None and code_nl.model is not None:
#             self.logger.info("CodeNL filter loaded from %s", code_nl.model_path)
#         elif not HAS_FASTTEXT:
#             self.logger.warning("fasttext not installed -- CodeNL DISABLED.")
#         else:
#             self.logger.warning("CodeNL model not found at %s -- CodeNL DISABLED.",
#                                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "code_nl_model.bin"))

#         if AI_ENABLED:
#             self.logger.info("AI ENABLED (%s providers).", len(AI_PROVIDER_NAMES))
#             if self.target:
#                 self.logger.info("Target: '%s'", self.target)
#         else:
#             self.logger.warning("g4f not installed -- AI DISABLED.")

#     # ────────────────────────────────────────────────────────────────
#     # NEW static method
#     # ────────────────────────────────────────────────────────────────
#     @staticmethod
#     def is_leadership_page(text: str) -> bool:
#         """Detect leadership pages by content, not just URL."""
#         if not text:
#             return False
#         text_lower = text.lower()
#         score = sum(1 for signal in FinderSpider.LEADERSHIP_TEXT_SIGNALS if signal in text_lower)
#         return score >= 2

#     # ────────────────────────────────────────────────────────────────
#     # NEW: Sitemap parser, nav extractor, JSON‑LD extractor
#     # ────────────────────────────────────────────────────────────────

#     def _parse_sitemap(self, xml_text: str, base_url: str) -> List[str]:
#         """Parse sitemap.xml and return URLs matching leadership keywords."""
#         if not xml_text:
#             return []
#         try:
#             root = ET.fromstring(xml_text)
#         except ET.ParseError:
#             return []
#         namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
#         # Try to find sub-sitemaps first
#         sitemap_urls = []
#         for sm in root.findall('.//ns:loc', namespaces):
#             text = (sm.text or "").strip()
#             if text:
#                 sitemap_urls.append(text)
#         if not sitemap_urls:
#             sitemap_urls = [base_url]  # The XML itself is the urlset

#         leadership_urls = []
#         for url in sitemap_urls:
#             url_lower = url.lower()
#             if any(kw in url_lower for kw in self.LEADERSHIP_PATTERNS):
#                 leadership_urls.append(url)
#         return leadership_urls

#     def _extract_nav_links(self, response) -> List[str]:
#         """Extract only navigation/header links — highest probability of structure."""
#         links = set()
#         for nav in response.css("nav"):
#             for href in nav.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         for header in response.css("header"):
#             for href in header.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         for footer in response.css("footer"):
#             for href in footer.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         domain = self.normalize_domain(response.url)
#         filtered = []
#         for url in links:
#             if domain not in urlparse(url).netloc.lower():
#                 continue
#             if any(kw in url.lower() for kw in self.LEADERSHIP_PATTERNS):
#                 filtered.append(url)
#         return filtered

#     def _extract_jsonld_people(self, response) -> List[Tuple[str, str]]:
#         """Bypass LLM entirely if site uses schema.org/Person structured data."""
#         people = []
#         for script in response.css('script[type="application/ld+json"]::text').getall():
#             try:
#                 data = json.loads(script)
#                 items = data if isinstance(data, list) else [data]
#                 for item in items:
#                     if item.get("@type") == "Person":
#                         name = item.get("name", "").strip()
#                         title = item.get("jobTitle", "").strip() or item.get("worksFor", {}).get("jobTitle", "").strip()
#                         if name and title:
#                             people.append((name, title))
#                     if item.get("@type") == "Organization":
#                         for emp in item.get("employee", []):
#                             if isinstance(emp, dict) and emp.get("@type") == "Person":
#                                 name = emp.get("name", "").strip()
#                                 title = emp.get("jobTitle", "").strip()
#                                 if name and title:
#                                     people.append((name, title))
#             except (json.JSONDecodeError, TypeError):
#                 continue
#         return people

#     # ────────────────────────────────────────────────────────────────
#     # NEW: Sitemap response handler & error fallback
#     # ────────────────────────────────────────────────────────────────

#     def parse_sitemap(self, response):
#         """Handle sitemap.xml response."""
#         root_url = response.meta["root_url"]
#         domain = response.meta["domain"]
#         batch_index = response.meta["batch_index"]
#         leadership_urls = self._parse_sitemap(response.text, root_url)
#         if leadership_urls:
#             self.logger.info("SITEMAP: Found %s leadership URLs for %s", len(leadership_urls), domain)
#             for url in leadership_urls:
#                 if url not in self.visited:
#                     self.visited.add(url)
#                     yield scrapy.Request(
#                         url,
#                         callback=self.parse_page,
#                         errback=self.handle_request_error,
#                         priority=5,
#                         meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#                     )
#         else:
#             self.logger.info("SITEMAP: No leadership URLs found for %s, falling back to homepage", domain)
#             yield scrapy.Request(
#                 root_url,
#                 callback=self.parse_page,
#                 errback=self.handle_request_error,
#                 meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#             )

#     def _sitemap_error(self, failure):
#         """Sitemap.xml failed (404, timeout, etc.) — fall back to homepage."""
#         root_url = failure.request.meta["root_url"]
#         domain = failure.request.meta["domain"]
#         batch_index = failure.request.meta["batch_index"]
#         self.logger.info("SITEMAP: Failed for %s, falling back to homepage", domain)
#         yield scrapy.Request(
#             root_url,
#             callback=self.parse_page,
#             errback=self.handle_request_error,
#             meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#         )

#     # ────────────────────────────────────────────────────────────────
#     # Batch and request handling – modified to use sitemap first
#     # ────────────────────────────────────────────────────────────────

#     def _start_batch(self, batch_index):
#         start = batch_index * self.batch_size
#         end = min(start + self.batch_size, len(self.all_urls))
#         batch_urls = self.all_urls[start:end]
#         if not batch_urls:
#             return
#         self.current_batch = batch_index
#         self.logger.info("Starting batch %s with %s URLs", batch_index + 1, len(batch_urls))

#         for url in batch_urls:
#             domain = self.normalize_domain(url)
#             parsed = urlparse(url)
#             sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
#             # Try sitemap first
#             yield scrapy.Request(
#                 sitemap_url,
#                 callback=self.parse_sitemap,
#                 errback=self._sitemap_error,
#                 meta={"root_url": url, "domain": domain, "batch_index": batch_index},
#                 priority=10,
#             )

#     # ────────────────────────────────────────────────────────────────
#     # start_requests – keep legacy batched logic (commented out old)
#     # ────────────────────────────────────────────────────────────────

#     # def start_requests(self):
#     #     yield from self._start_batch(0)

#     def start_requests(self):
#         """Start by fetching sitemap.xml for each domain, then fall back to homepage."""
#         for url in self.all_urls:
#             domain = self.normalize_domain(url)
#             parsed = urlparse(url)
#             sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
#             yield scrapy.Request(
#                 sitemap_url,
#                 callback=self.parse_sitemap,
#                 errback=self._sitemap_error,
#                 meta={"root_url": url, "domain": domain, "batch_index": 0},
#                 priority=10,
#             )

#     def spider_idle(self, spider):
#         next_batch = self.current_batch + 1
#         if next_batch * self.batch_size >= len(self.all_urls):
#             return
#         self.logger.info("Spider idle, starting next batch %s", next_batch + 1)
#         for request in self._start_batch(next_batch):
#             try:
#                 self.crawler.engine.crawl(request)
#             except TypeError:
#                 self.crawler.engine.crawl(request, spider)
#         raise DontCloseSpider

#     # ────────────────────────────────────────────────────────────────
#     # Helper methods unchanged
#     # ────────────────────────────────────────────────────────────────

#     @staticmethod
#     def normalize_domain(url):
#         return urlparse(url).netloc.lower().replace("www.", "").strip()

#     @staticmethod
#     def normalize_url(raw_url):
#         if pd.isna(raw_url):
#             return None
#         url = str(raw_url).strip()
#         if not url:
#             return None
#         if not url.startswith(("http://", "https://")):
#             url = f"http://{url}"
#         parsed = urlparse(url)
#         if not parsed.netloc or " " in parsed.netloc:
#             return None
#         return url

#     @classmethod
#     def is_trash_email(cls, email):
#         value = email.strip().lower()
#         if "@" not in value:
#             return True
#         local, domain = value.rsplit("@", 1)
#         domain = domain.lstrip(".")
#         if domain.startswith("www."):
#             domain = domain[4:]
#         if not local or not domain:
#             return True
#         if domain in cls.TRASH_EMAIL_DOMAINS or domain.startswith("example."):
#             return True
#         if domain.endswith(".sentry.io") or "mysite" in domain:
#             return True
#         if domain.endswith("wixpress.com"):
#             return True
#         if local in cls.TRASH_EMAIL_LOCAL_PARTS:
#             return True
#         if re.fullmatch(r"[0-9a-f]{24,}", local):
#             return True
#         if re.fullmatch(r"[0-9a-f-]{30,}", local):
#             return True
#         tld = domain.rsplit(".", 1)[-1]
#         if not re.fullmatch(r"[a-z]{2,10}", tld):
#             return True
#         if value.endswith(cls.IMAGE_SUFFIXES):
#             return True
#         return False

#     def email_domain_has_dns(self, domain):
#         key = domain.strip().lower()
#         if not key:
#             return False
#         cached = self.email_domain_dns_cache.get(key)
#         if cached is not None:
#             return cached
#         is_valid = False
#         try:
#             mx_records = self.dns_resolver.resolve(key, "MX")
#             is_valid = bool(mx_records)
#         except Exception:
#             try:
#                 a_records = self.dns_resolver.resolve(key, "A")
#                 is_valid = bool(a_records)
#             except Exception:
#                 is_valid = False
#         self.email_domain_dns_cache[key] = is_valid
#         return is_valid

#     def load_keywords(self):
#         project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
#         keyword_path = None
#         for file in os.listdir(project_root):
#             if file.lower() == "keywords.json":
#                 keyword_path = os.path.join(project_root, file)
#                 break
#         if not keyword_path:
#             raise FileNotFoundError(f"Keywords.json file not found in project root: {project_root}")
#         with open(keyword_path, "r", encoding="utf-8") as file:
#             data = json.load(file)
#         include_raw = data.get("include_keywords", [])
#         self.keyword_category = {}
#         if isinstance(include_raw, dict):
#             include = {}
#             for category, keywords in include_raw.items():
#                 category_keywords = [kw.lower() for kw in (keywords or [])]
#                 include[category] = category_keywords
#                 for kw in category_keywords:
#                     self.keyword_category[kw] = category
#         else:
#             include = {"General": [kw.lower() for kw in include_raw]}
#             self.keyword_category = {kw: "General" for kw in include["General"]}
#         block = [kw.lower() for kw in data.get("block_keywords", [])]
#         return include, block

#     def load_input_files(self):
#         input_folder = os.path.join(
#             os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Input"
#         )
#         urls = []
#         for file in sorted(os.listdir(input_folder)):
#             path = os.path.join(input_folder, file)
#             try:
#                 if file.endswith(".csv"):
#                     df = pd.read_csv(path, sep=None, engine="python", on_bad_lines="warn")
#                 elif file.endswith(".xlsx") or file.endswith(".xls"):
#                     df = pd.read_excel(path)
#                 else:
#                     continue
#             except Exception as error:
#                 self.logger.warning("Failed to read %s: %s", file, error)
#                 continue

#             url_column = None
#             phone_column = None
#             company_column = None
#             for col in df.columns:
#                 col_name = str(col).strip().lower()
#                 if col_name in ["website", "url", "website url"] and url_column is None:
#                     url_column = col
#                 if ("phone" in col_name or "contact" in col_name) and phone_column is None:
#                     phone_column = col
#                 if col_name in ["company name", "agency name", "name", "business name"] and company_column is None:
#                     company_column = col

#             if url_column is None:
#                 self.logger.warning("No URL column found in %s", file)
#                 continue

#             for _, row in df.iterrows():
#                 normalized_url = self.normalize_url(row[url_column])
#                 if not normalized_url:
#                     continue
#                 urls.append(normalized_url)
#                 domain = self.normalize_domain(normalized_url)

#                 if domain not in self.domain_originals:
#                     self.domain_originals[domain] = {}
#                     for col in df.columns:
#                         val = row.get(col)
#                         if pd.notna(val):
#                             self.domain_originals[domain][col] = str(val).strip()

#                 if domain not in self.domain_websites:
#                     self.domain_websites[domain] = normalized_url
#                 if phone_column is not None:
#                     phone = row[phone_column]
#                     if pd.notna(phone):
#                         phone_value = str(phone).strip()
#                         if phone_value and phone_value.lower() != "nan":
#                             self.domain_phones[domain] = phone_value
#                 if company_column is not None and domain not in self.domain_companies:
#                     company = row[company_column]
#                     if pd.notna(company):
#                         company_value = str(company).strip()
#                         if company_value and company_value.lower() != "nan":
#                             self.domain_companies[domain] = company_value
#         return list(set(urls))

#     @classmethod
#     def _decode_js_escapes(cls, source_text):
#         if not source_text:
#             return ""
#         decoded = cls.HEX_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), source_text,
#         )
#         decoded = cls.UNICODE_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), decoded,
#         )
#         return decoded

#     @classmethod
#     def _extract_emails_from_source(cls, source_text):
#         if not source_text:
#             return set()
#         blobs = []
#         raw = source_text
#         html_unescaped = unescape(raw)
#         url_decoded = unquote(html_unescaped)
#         js_decoded = cls._decode_js_escapes(url_decoded)
#         for blob in (raw, html_unescaped, url_decoded, js_decoded):
#             if blob and blob not in blobs:
#                 blobs.append(blob)
#         found = set()
#         for blob in blobs:
#             for email in cls.EMAIL_REGEX.findall(blob):
#                 found.add(email.strip().lower())
#             for email in cls.MAILTO_REGEX.findall(blob):
#                 normalized = unquote(email).strip().lower().replace("%40", "@")
#                 found.add(normalized)
#             for match in cls.OBFUSCATED_EMAIL_REGEX.finditer(blob):
#                 local = match.group(1).strip().lower()
#                 domain_part = match.group(2).strip().lower().strip(".")
#                 tld = match.group(3).strip().lower()
#                 found.add(f"{local}@{domain_part}.{tld}")
#         return found

#     def _build_email_candidates(self, source_text):
#         candidates = []
#         seen = set()
#         extracted = self._extract_emails_from_source(source_text)
#         for email in extracted:
#             normalized_email = email.strip().lower().replace("mailto:", "").strip(" <>\"'(),;")
#             if not normalized_email or normalized_email in seen:
#                 continue
#             if self.is_trash_email(normalized_email):
#                 continue
#             seen.add(normalized_email)
#             candidates.append(normalized_email)
#         return candidates

#     def _extract_socials(self, response, domain):
#         data = self.domain_data[domain]
#         for link in response.css("a::attr(href)").getall():
#             full_url = response.urljoin(link)
#             parsed = urlparse(full_url)
#             netloc = parsed.netloc.lower().replace("www.", "")
#             if not netloc:
#                 continue
#             matched = False
#             for platform, domains in self.SOCIAL_DOMAINS.items():
#                 if any(d in netloc for d in domains):
#                     data["socials"][platform].add(full_url)
#                     matched = True
#                     break
#             if not matched:
#                 if any(d in netloc for d in self.OTHER_SOCIAL_DOMAINS):
#                     data["socials"]["other"].add(full_url)

#     def _extract_tel_phones(self, source_text, domain):
#         data = self.domain_data[domain]
#         for match in self.TEL_HREF_REGEX.findall(source_text):
#             raw = match.strip()
#             cleaned = re.sub(r"[^\d+]", "", raw)
#             if cleaned:
#                 data["phones_scraped"].add(cleaned)

#     def handle_request_error(self, failure):
#         request = failure.request
#         self.logger.warning("Request failed: %s (%s)", request.url, failure.value)

#     # ────────────────────────────────────────────────────────────────
#     # parse_page – UPDATED VERSION (new)
#     # ────────────────────────────────────────────────────────────────

#     async def parse_page(self, response):
#         root_url = response.meta["root_url"]
#         domain = self.normalize_domain(root_url)
#         batch_index = response.meta.get("batch_index", self.current_batch)
#         depth = response.meta.get("depth", 0)

#         outputs = []

#         try:
#             is_new_domain = domain not in self.domain_data
#             if is_new_domain:
#                 self.domain_data[domain] = {
#                     "include_count": 0,
#                     "matched_keywords": set(),
#                     "matched_categories": set(),
#                     "blocked": False,
#                     "emails": set(),
#                     "leadership_text": "",
#                     "general_text": "",
#                     "leadership_seeded": False,
#                     "socials": {
#                         "twitter": set(), "linkedin": set(), "facebook": set(),
#                         "instagram": set(), "youtube": set(), "other": set(),
#                     },
#                     "phones_scraped": set(),
#                 }

#             source_text = response.text
#             text = source_text.lower()

#             for keyword in self.block_keywords:
#                 if keyword in text:
#                     self.domain_data[domain]["blocked"] = True
#                     return outputs

#             all_keywords = [
#                 keyword
#                 for keywords in self.include_keywords.values()
#                 for keyword in keywords
#             ]

#             count = sum(text.count(keyword) for keyword in all_keywords)
#             self.domain_data[domain]["include_count"] += count

#             for keyword in all_keywords:
#                 if keyword in text:
#                     self.domain_data[domain]["matched_keywords"].add(keyword)
#                     category = self.keyword_category.get(keyword)
#                     if category:
#                         self.domain_data[domain]["matched_categories"].add(category)

#             # ═══════════════════════════════════════════════════════
#             # NEW: Content-based leadership detection + URL-based
#             # ═══════════════════════════════════════════════════════
#             url_lower = response.url.lower()
#             url_is_leadership = any(pat in url_lower for pat in self.LEADERSHIP_PATTERNS)
#             content_is_leadership = self.is_leadership_page(source_text)
#             is_leadership_page = url_is_leadership or content_is_leadership

#             if is_leadership_page:
#                 self.logger.info("LEADERSHIP PAGE DETECTED: %s", response.url)

#             if AI_ENABLED and len(self.domain_data[domain]["leadership_text"]) + len(self.domain_data[domain]["general_text"]) < self.AI_PAGE_TEXT_CAP:
#                 cleaned = _ai_clean_text(source_text)
#                 # Tag each chunk with its source URL and page type
#                 tagged = f"\n\n--- PAGE: {response.url} | TYPE: {'LEADERSHIP' if is_leadership_page else 'GENERAL'} ---\n\n{cleaned}"
#                 if is_leadership_page:
#                     current = self.domain_data[domain]["leadership_text"]
#                     cap = self.AI_PAGE_TEXT_CAP // 2
#                     self.domain_data[domain]["leadership_text"] = (current + tagged)[:cap]
#                 else:
#                     current = self.domain_data[domain]["general_text"]
#                     cap = self.AI_PAGE_TEXT_CAP // 2
#                     self.domain_data[domain]["general_text"] = (current + tagged)[:cap]

#             # Deterministic extraction on every page
#             self._extract_socials(response, domain)
#             self._extract_tel_phones(source_text, domain)

#             candidates = self._build_email_candidates(source_text)

#             results = []
#             if candidates:
#                 if self.settings.getbool("BRUTE_EMAIL_VALIDATE_DNS", False):
#                     deferreds = []
#                     for email in candidates:
#                         deferreds.append(threads.deferToThread(self._validate_email_dns, email))
#                     dlist = DeferredList(deferreds, consumeErrors=True)
#                     results = await maybe_deferred_to_future(dlist)
#                 else:
#                     results = [(True, email) for email in candidates]

#             more_outputs = await self._after_dns_checks(results, response, domain, batch_index, depth)
#             outputs.extend(more_outputs)

#             # ═══════════════════════════════════════════════════════
#             # NEW: Manual leadership URL generation with HIGH priority
#             # ═══════════════════════════════════════════════════════
#             if is_new_domain and not self.domain_data[domain]["leadership_seeded"]:
#                 self.domain_data[domain]["leadership_seeded"] = True
#                 base = response.url.rstrip("/")
#                 for path in self.LEADERSHIP_SEED_PATHS:
#                     seed_url = f"{base}/{path}"
#                     if seed_url not in self.visited:
#                         self.visited.add(seed_url)
#                         outputs.append(scrapy.Request(
#                             seed_url,
#                             callback=self.parse_page,
#                             errback=self.handle_request_error,
#                             priority=200,           # ← HIGHEST: before any other link
#                             meta={
#                                 "depth": depth + 1,
#                                 "root_url": root_url,
#                                 "batch_index": batch_index,
#                             },
#                         ))

#         finally:
#             self.logger.debug("Request finished: %s", response.url)

#         return outputs

#     # ───── OLD parse_page (commented out) ─────
#     """
#     async def parse_page(self, response):
#         ... old implementation ...
#     """

#     def _validate_email_dns(self, email):
#         email_domain = email.rsplit("@", 1)[-1]
#         if self.email_domain_has_dns(email_domain):
#             return email
#         return None

#     # ────────────────────────────────────────────────────────────────
#     # _after_dns_checks – unchanged
#     # ────────────────────────────────────────────────────────────────

#     async def _after_dns_checks(self, results, response, domain, batch_index, depth):
#         for success, email_or_none in results:
#             if success and email_or_none:
#                 self.domain_data[domain]["emails"].add(email_or_none)

#         outputs = []
#         data = self.domain_data[domain]
#         if data["blocked"] or data["include_count"] < 2:
#             if depth < 10:
#                 outputs.extend(self._get_follow_requests(response, domain, batch_index, depth))
#             return outputs

#         if domain not in self.yielded_domains:
#             self.yielded_domains.add(domain)
#             item = await self._enrich_and_build_item(domain)
#             outputs.append(item)

#         if depth < 10:
#             outputs.extend(self._get_follow_requests(response, domain, batch_index, depth))
#         return outputs

#     # ────────────────────────────────────────────────────────────────
#     # _get_follow_requests – UPDATED VERSION (new)
#     # ────────────────────────────────────────────────────────────────

#     def _get_follow_requests(self, response, domain, batch_index, depth):
#         """Generate follow requests with leadership priority scoring."""
#         root_url = response.meta["root_url"]
#         links = response.css("a::attr(href)").getall()

#         def _url_priority(url):
#             url_lower = url.lower()
#             for keyword, score in self.LEADERSHIP_PRIORITY.items():
#                 if keyword in url_lower:
#                     return score
#             # Bonus for any follow pattern
#             if any(pat in url_lower for pat in self.FOLLOW_URL_PATTERNS):
#                 return 10
#             return 0

#         # Sort so leadership links are processed first
#         links.sort(key=_url_priority, reverse=True)
#         requests = []

#         for link in links:
#             next_url = response.urljoin(link)

#             if domain not in urlparse(next_url).netloc.lower():
#                 continue
#             if next_url.lower().endswith((".jpg", ".png", ".pdf", ".zip", ".gif", ".svg", ".webp")):
#                 continue
#             if not any(item in next_url.lower() for item in self.FOLLOW_URL_PATTERNS):
#                 continue
#             if next_url in self.visited:
#                 continue

#             self.visited.add(next_url)
#             priority = _url_priority(next_url)

#             requests.append(scrapy.Request(
#                 next_url,
#                 callback=self.parse_page,
#                 errback=self.handle_request_error,
#                 priority=priority,          # ← leadership pages get 70-100 priority
#                 meta={
#                     "depth": depth + 1,
#                     "root_url": root_url,
#                     "batch_index": batch_index,
#                 },
#             ))

#         return requests

#     # ───── OLD _get_follow_requests (commented out) ─────
#     """
#     def _get_follow_requests(self, response, domain, batch_index, depth):
#         ... old implementation ...
#     """

#     # ────────────────────────────────────────────────────────────────
#     # _enrich_and_build_item – with JSON‑LD priority
#     # ────────────────────────────────────────────────────────────────

#     async def _enrich_and_build_item(self, domain):
#         ai_data = None
#         jsonld_people = self.domain_data[domain].get("jsonld_people", [])
#         if jsonld_people:
#             self.logger.info("Using JSON‑LD data for %s, skipping LLM", domain)
#             names = [name for name, _ in jsonld_people]
#             titles = [title for _, title in jsonld_people]
#             ai_data = {
#                 "person_name": ", ".join(names),
#                 "job_title": ", ".join(titles),
#                 "service": "Not Found",
#                 "relevancy": "True",
#                 "sales_hook": "Not Found",
#             }
#         else:
#             if AI_ENABLED and domain not in self.enriched_domains:
#                 self.enriched_domains.add(domain)
#                 company_name = self.domain_companies.get(domain, "")
#                 leadership_text = self.domain_data[domain]["leadership_text"]
#                 general_text = self.domain_data[domain]["general_text"]
#                 self.logger.info("Running AI lead extraction for domain: %s", domain)
#                 ai_data = await maybe_deferred_to_future(
#                     threads.deferToThread(
#                         ai_get_lead_info, domain, company_name,
#                         leadership_text, general_text, self.target
#                     )
#                 )
#                 gc.collect()
#         return self.build_item(domain, ai_data)

#     def build_item(self, domain, ai_data=None):
#         data = self.domain_data[domain]
#         original = self.domain_originals.get(domain, {})
#         ai_data = ai_data or {}

#         def orig(field, default=""):
#             val = original.get(field, "")
#             return val if val and val.lower() not in ("nan",) else default

#         def merge(field, ai_key=None, scraped=""):
#             o = orig(field)
#             if o and o not in ("Not Found", "False"):
#                 return o
#             if ai_key and ai_data.get(ai_key) and ai_data[ai_key] not in ("Not Found", "False"):
#                 return ai_data[ai_key]
#             if scraped:
#                 return scraped
#             return "Not Found"

#         orig_rel = orig("Relevancy")
#         if orig_rel and orig_rel not in ("Not Found", "False"):
#             relevancy = orig_rel
#         else:
#             relevancy = ai_data.get("relevancy", "False")
#         socials = data["socials"]

#         item = {
#             "Website URL": domain,
#             "Company Name": orig("Company Name", self.domain_companies.get(domain, "")),
#             "Phone Number": orig("Phone Number", self.domain_phones.get(domain, "")),
#             "Additional Phone Numbers": ", ".join(sorted(data["phones_scraped"])) if data["phones_scraped"] else "",
#             "Person Name": merge("Person Name", "person_name"),
#             "Job Title": merge("Job Title", "job_title"),
#             "Relevancy": relevancy,
#             "Service": merge("Service", "service"),
#             "Sales Hook": ai_data.get("sales_hook", "Not Found"),
#             "Keywords - Team": orig("Keywords - Team", ",".join(sorted(data["matched_keywords"]))),
#             "Specification": orig("Specification", ",".join(sorted(data["matched_categories"]))),
#             "Emails": orig("Emails", ",".join(sorted(data["emails"]))),
#             "Twitter": ", ".join(sorted(socials["twitter"])),
#             "LinkedIn": ", ".join(sorted(socials["linkedin"])),
#             "Facebook": ", ".join(sorted(socials["facebook"])),
#             "Instagram": ", ".join(sorted(socials["instagram"])),
#             "YouTube": ", ".join(sorted(socials["youtube"])),
#             "Other Social": ", ".join(sorted(socials["other"])),
#         }

#         self.logger.info(
#             "YIELDING ITEM for domain: %s | Person: %s | Job: %s | Service: %s | Relevancy: %s | Hook: %s",
#             domain,
#             item["Person Name"], item["Job Title"], item["Service"],
#             item["Relevancy"], item["Sales Hook"],
#         )
#         return item









# import os
# os.environ["SE_HEADLESS"] = "1"
# os.environ["PLAYWRIGHT_HEADLESS"] = "1"
# os.environ["CHROME_HEADLESS"] = "1"
# os.environ["MOZ_HEADLESS"] = "1"
# os.environ["PYPPETEER_HEADLESS"] = "1"
# os.environ["HEADLESS"] = "1"
# import gc
# import json
# import random
# import re
# import time
# import warnings
# import concurrent.futures
# from html import unescape
# from typing import Any, Dict, List, Optional, Tuple
# from urllib.parse import unquote, urlparse, urljoin
# from xml.etree import ElementTree as ET

# import dns.resolver
# import pandas as pd
# import scrapy
# from scrapy import signals
# from scrapy.exceptions import DontCloseSpider
# from scrapy.utils.defer import maybe_deferred_to_future
# from twisted.internet import threads
# from twisted.internet.defer import DeferredList

# try:
#     import g4f
#     from g4f import Provider
#     from g4f.client import Client
#     HAS_G4F = True
# except ImportError:
#     HAS_G4F = False

# try:
#     from bs4 import BeautifulSoup
#     HAS_BS4 = True
# except ImportError:
#     HAS_BS4 = False

# try:
#     import fasttext
#     HAS_FASTTEXT = True
# except ImportError:
#     HAS_FASTTEXT = False

# warnings.filterwarnings("ignore", message="Unclosed client session")
# warnings.filterwarnings("ignore", message="Unclosed connector")

# AI_ENABLED = HAS_G4F
# AI_PROVIDER_NAMES = [
#     "Cloudflare",
#     "CohereForAI_C4AI_Command",
#     "AnyProvider",
#     "Gemini",
#     "Perplexity",
#     "Yqcloud",
#     "OpenaiChat",
#     "MiniMax",
#     "WhiteRabbitNeo",
# ]
# AI_FALLBACK_MODEL = "gpt-4o-mini"
# AI_PER_CALL_TIMEOUT = 35
# AI_RACE_TIMEOUT = 50
# AI_MAX_RETRIES = 5
# AI_RETRY_BASE_DELAY = 2.0
# AI_MAX_WORKERS = min(12, len(AI_PROVIDER_NAMES))
# AI_MAX_TEXT_CHARS = 15000
# AI_CHUNK_MIN_LENGTH = 50
# AI_CHUNK_MAX_LENGTH = 1200
# AI_CODENL_THRESHOLD = 0.85
# AI_REQUIRED_KEYS = {"person_name", "job_title", "service", "relevancy", "sales_hook"}

# class CodeNLFilter:
#     def __init__(self, model_path: str, threshold: float = AI_CODENL_THRESHOLD):
#         self.model_path = model_path
#         self.threshold = threshold
#         self.model: Optional[fasttext.FastText] = None
#         self._load_model()

#     def _load_model(self) -> None:
#         if not HAS_FASTTEXT:
#             return
#         if not os.path.exists(self.model_path):
#             return
#         try:
#             self.model = fasttext.load_model(self.model_path)
#         except Exception:
#             self.model = None

#     def is_code(self, text: str) -> Tuple[bool, float]:
#         if self.model is None or not text or len(text) < 10:
#             return False, 0.0
#         try:
#             labels, probs = self.model.predict(text.replace("\n", " "), k=1)
#             label = labels[0].replace("__label__", "")
#             confidence = float(probs[0])
#             is_code = (label.lower() == "code" and confidence >= self.threshold)
#             return is_code, confidence
#         except Exception:
#             return False, 0.0

#     def filter_text(self, text: str) -> str:
#         if self.model is None or not text:
#             return text
#         chunks = self._semantic_chunk(text)
#         clean_chunks: List[str] = []
#         for chunk in chunks:
#             is_code, _ = self.is_code(chunk)
#             if not is_code:
#                 clean_chunks.append(chunk)
#         return "\n".join(clean_chunks)

#     @staticmethod
#     def _semantic_chunk(text: str) -> List[str]:
#         paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
#         chunks: List[str] = []
#         for para in paragraphs:
#             if len(para) < AI_CHUNK_MIN_LENGTH:
#                 continue
#             if len(para) <= AI_CHUNK_MAX_LENGTH:
#                 chunks.append(para)
#             else:
#                 sentences = re.split(r'(?<=[.!?])\s+', para)
#                 current = ""
#                 for sent in sentences:
#                     sent = sent.strip()
#                     if not sent:
#                         continue
#                     if len(current) + len(sent) + 1 <= AI_CHUNK_MAX_LENGTH:
#                         current = (current + " " + sent).strip() if current else sent
#                     else:
#                         if current and len(current) >= AI_CHUNK_MIN_LENGTH:
#                             chunks.append(current)
#                         current = sent
#                 if current and len(current) >= AI_CHUNK_MIN_LENGTH:
#                     chunks.append(current)
#         return chunks

# _code_nl_filter: Optional[CodeNLFilter] = None

# def _get_code_nl_filter() -> Optional[CodeNLFilter]:
#     global _code_nl_filter
#     if _code_nl_filter is not None:
#         return _code_nl_filter
#     if not HAS_FASTTEXT:
#         return None
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     model_path = os.path.join(script_dir, "code_nl_model.bin")
#     _code_nl_filter = CodeNLFilter(model_path, AI_CODENL_THRESHOLD)
#     return _code_nl_filter

# # ============================================================
# # _ai_clean_text (updated)
# # ============================================================
# def _ai_clean_text(html_text: str) -> str:
#     if not html_text:
#         return ""
#     if HAS_BS4:
#         soup = BeautifulSoup(html_text, "lxml")
#         for tag in soup(["script", "style", "noscript", "svg", "iframe", "nav", "footer"]):
#             tag.decompose()

#         for tag in soup.find_all(True):
#             if not hasattr(tag, "name") or tag.name is None:
#                 continue
#             if getattr(tag, "attrs", None) is None:
#                 continue
#             try:
#                 cls = " ".join(tag.get("class") or []).lower()
#                 id_attr = (tag.get("id") or "").lower()
#             except Exception:
#                 continue
#             if any(kw in cls or kw in id_attr for kw in [
#                 "testimonial", "case-study", "case_study", "review", "quote",
#                 "customer-story", "success-story", "client-logo", "partner-logo",
#                 "client-list", "partner-list", "trusted-by", "used-by",
#             ]):
#                 tag.decompose()

#         text = soup.get_text(separator=" ", strip=True)
#     else:
#         text = re.sub(r"<[^>]+>", " ", html_text)
#     text = re.sub(r"\s+", " ", text).strip()

#     code_nl = _get_code_nl_filter()
#     if code_nl is not None and code_nl.model is not None:
#         text = code_nl.filter_text(text)

#     return text

# # ============================================================
# # _ai_build_prompt (updated)
# # ============================================================
# def _ai_build_prompt(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> str:
#     target_str = target if target else "general business relevance"
#     example = (
#         '{"people":[' 
#         '{"person_name":"Alex Smith","job_title":"Founder & CEO","context":"Alex Smith, Founder & CEO, leads our team"},'
#         '{"person_name":"Max Doe","job_title":"CTO","context":"Max Doe serves as CTO"}'
#         '],"service":"AI recruitment software","relevancy":true,'
#         '"sales_hook":"What is the biggest bottleneck in scaling your AI recruitment platform?"}'
#     )

#     parts = [
#         "You are a strict B2B lead researcher. Extract ONLY current employees of THIS company.",
#         "",
#         "=== ABSOLUTE RULES — VIOLATING ANY OF THESE IS A FAILURE ===",
#         "1. people: JSON array of objects. Each object MUST have:",
#         "   - person_name: FULL NAME only. No company names, no quotes, no descriptors.",
#         "   - job_title: Their EXACT internal job title at THIS company ONLY.",
#         "   - context: The EXACT sentence/phrase from the text where this person appears with their title.",
#         "",
#         "2. REJECT and NEVER extract any of the following:",
#         "   - Customer testimonial givers (e.g. 'Jane Doe, CMO at ClientCorp')",
#         "   - Case study subjects or featured clients",
#         "   - Partner company employees or partner logos",
#         "   - Advisory board members, board of directors, or external advisors UNLESS they are full-time employees",
#         "   - Investors, VCs, or funding partners",
#         "   - Former employees, ex-employees, or alumni",
#         "   - Anyone whose title mentions another company name (e.g. 'CMO, Vercel' or 'Manager at Zendesk')",
#         "   - Anyone appearing in a quote, review, or testimonial section",
#         "   - Generic placeholders like 'John Doe' or 'Jane Smith'",
#         "",
#         "3. A person MUST be a current internal employee of the company being scraped.",
#         "   - Titles like 'CMO, Vercel' or 'Director at Google' = EXTERNAL → REJECT",
#         "   - Titles like 'Our client, Bob from Acme' = EXTERNAL → REJECT",
#         "   - Titles like 'Former CTO' or 'Ex-CEO' = EXTERNAL → REJECT",
#         "   - Titles with '@' and another company name (e.g. 'CEO @ MealBox') = EXTERNAL → REJECT",
#         "",
#         "4. If you are unsure whether a person is an employee, REJECT them.",
#         "",
#         "5. service: What this company sells/does in 2-8 words. Be specific.",
#         "",
#         '6. relevancy: true or false. Does this company match: "' + target_str + '"?',
#         "   - true = clearly fits. false = does not fit or unclear.",
#         "",
#         "7. sales_hook: ONE concise first question (max 12 words) for the MOST SENIOR employee.",
#         "",
#         "Respond with ONLY a single-line valid JSON object. No markdown, no explanation.",
#         "",
#         "Example: " + example,
#         "",
#         "=== LEADERSHIP / TEAM PAGES (highest priority — extract from these first) ===",
#         '"""' + (leadership_text[:8000] if leadership_text else "(no leadership text scraped)") + '"""',
#         "",
#         "=== GENERAL SITE PAGES (lower priority — use only if leadership pages are empty) ===",
#         '"""' + (general_text[:6000] if general_text else "(no general text scraped)") + '"""',
#     ]
#     return "\n".join(parts)

# # ============================================================
# # _ai_parse_json (updated)
# # ============================================================
# def _ai_parse_json(raw: str) -> Optional[Dict[str, Any]]:
#     if not raw:
#         return None
#     raw = raw.strip()
#     raw = re.sub(r"^```(json)?", "", raw, flags=re.IGNORECASE).strip()
#     raw = re.sub(r"```$", "", raw).strip()
#     match = re.search(r"\{.*\}", raw, re.DOTALL)
#     if not match:
#         return None
#     try:
#         data = json.loads(match.group(0))
#     except json.JSONDecodeError:
#         return None

#     if not isinstance(data, dict):
#         return None

#     # ── POST-VALIDATION: reject testimonial / customer / partner names ──
#     REJECT_TITLE_KEYWORDS = {
#         " at ", " of ", " from ", "formerly", "ex-", "ex ", "former ", "client",
#         "customer", "partner", "partnered", "testimonial", "advisory", "investor",
#         "board member", "external", "alumni", "retired",
#     }
#     REJECT_CONTEXT_KEYWORDS = {
#         "testimonial", "client", "customer", "partner", "partnered with",
#         "case study", "our client", "our customer", "success story",
#         "featured", "review", "quote from", "says ", "said ", "trusted by",
#     }
#     PLACEHOLDER_NAMES = {"john doe", "jane smith", "jane doe", "john smith", "placeholder"}

#     result = {}

#     people_list = data.get("people", [])
#     if isinstance(people_list, list) and people_list:
#         names = []
#         titles = []
#         for p in people_list:
#             if not isinstance(p, dict):
#                 continue

#             pname = str(p.get("person_name", "")).strip()
#             ptitle = str(p.get("job_title", "")).strip()
#             pcontext = str(p.get("context", "")).strip().lower()

#             if not pname or not ptitle:
#                 continue
#             if pname.lower() in PLACEHOLDER_NAMES:
#                 continue
#             if ptitle.lower() in ("null", "none", "n/a", "unknown", "not found"):
#                 continue

#             title_lower = ptitle.lower()
#             if any(kw in title_lower for kw in REJECT_TITLE_KEYWORDS):
#                 continue
#             if any(kw in pcontext for kw in REJECT_CONTEXT_KEYWORDS):
#                 continue
#             if re.search(r",\s*[A-Z][a-zA-Z]+", ptitle):
#                 continue
#             if re.search(r"@\s*[A-Z]", ptitle):
#                 continue

#             names.append(pname)
#             titles.append(ptitle)

#         result["person_name"] = ", ".join(names) if names else "Not Found"
#         result["job_title"] = ", ".join(titles) if titles else "Not Found"
#     else:
#         for key in ("person_name", "job_title"):
#             val = str(data.get(key, "")).strip()
#             if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#                 result[key] = "Not Found"
#             else:
#                 result[key] = val

#     for key in ("service", "sales_hook"):
#         val = str(data.get(key, "")).strip()
#         if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
#             result[key] = "Not Found"
#         else:
#             result[key] = val

#     rel = str(data.get("relevancy", "")).strip().lower()
#     result["relevancy"] = "True" if rel in ("true", "yes", "1", "high") else "False"

#     if not AI_REQUIRED_KEYS.issubset(result.keys()):
#         return None

#     return result

# def _ai_get_provider_class(name: str):
#     return getattr(Provider, name, None)

# def _ai_pick_model(provider_class) -> str:
#     models = getattr(provider_class, "models", None)
#     return models[0] if models else AI_FALLBACK_MODEL

# def _ai_call_provider(provider_name: str, prompt: str) -> Optional[Dict[str, Any]]:
#     provider_class = _ai_get_provider_class(provider_name)
#     if provider_class is None:
#         return None
#     model = _ai_pick_model(provider_class)
#     client = Client()
#     try:
#         response = client.chat.completions.create(
#             model=model,
#             provider=provider_class,
#             messages=[{"role": "user", "content": prompt}],
#             stream=False,
#             timeout=AI_PER_CALL_TIMEOUT,
#         )
#         content = None
#         if getattr(response, "choices", None):
#             message = response.choices[0].message
#             content = getattr(message, "content", None)
#         if not isinstance(content, str):
#             return None
#         return _ai_parse_json(content)
#     except Exception:
#         return None
#     finally:
#         del client

# def _ai_race_providers(prompt: str) -> Optional[Dict[str, Any]]:
#     with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as executor:
#         future_to_name = {
#             executor.submit(_ai_call_provider, name, prompt): name
#             for name in AI_PROVIDER_NAMES
#         }
#         winner = None
#         try:
#             for future in concurrent.futures.as_completed(future_to_name, timeout=AI_RACE_TIMEOUT):
#                 try:
#                     result = future.result()
#                 except Exception:
#                     result = None
#                 if result is not None:
#                     winner = result
#                     break
#         except concurrent.futures.TimeoutError:
#             pass
#         executor.shutdown(wait=False, cancel_futures=True)
#     return winner

# def ai_get_lead_info(domain: str, company_name: str, leadership_text: str,
#                      general_text: str, target: str) -> Dict[str, str]:
#     if not AI_ENABLED:
#         return {
#             "person_name": "Not Found", "job_title": "Not Found",
#             "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#         }
#     prompt = _ai_build_prompt(domain, company_name, leadership_text, general_text, target)
#     for attempt in range(1, AI_MAX_RETRIES + 1):
#         result = _ai_race_providers(prompt)
#         if result is not None:
#             return result
#         delay = AI_RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
#         time.sleep(min(delay, 20))
#         gc.collect()
#     return {
#         "person_name": "Not Found", "job_title": "Not Found",
#         "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
#     }

# # ============================================================
# # SPIDER
# # ============================================================

# class FinderSpider(scrapy.Spider):
#     name = "Gliner_Spider"

#     EMAIL_REGEX = re.compile(
#         r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}",
#         re.IGNORECASE,
#     )
#     MAILTO_REGEX = re.compile(
#         r"mailto:\s*([A-Za-z0-9._%+\-]+(?:%40|@)[A-Za-z0-9.\-]+\.[A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     OBFUSCATED_EMAIL_REGEX = re.compile(
#         r"([A-Za-z0-9._%+\-]{1,64})\s*"
#         r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\}|\s+at\s+|@)\s*"
#         r"([A-Za-z0-9.\-]{1,253})\s*"
#         r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\s+dot\s+|\.)\s*"
#         r"([A-Za-z]{2,10})",
#         re.IGNORECASE,
#     )
#     HEX_ESCAPE_REGEX = re.compile(r"\\x([0-9a-fA-F]{2})")
#     UNICODE_ESCAPE_REGEX = re.compile(r"\\u([0-9a-fA-F]{4})")
#     TEL_HREF_REGEX = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)

#     SOCIAL_DOMAINS = {
#         "twitter": ["twitter.com", "x.com"],
#         "linkedin": ["linkedin.com"],
#         "facebook": ["facebook.com", "fb.com"],
#         "instagram": ["instagram.com"],
#         "youtube": ["youtube.com", "youtu.be"],
#     }
#     OTHER_SOCIAL_DOMAINS = [
#         "tiktok.com", "pinterest.com", "reddit.com", "github.com",
#         "medium.com", "snapchat.com", "discord.com", "discord.gg",
#         "t.me", "telegram.me", "whatsapp.com", "wa.me",
#     ]

#     IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico")
#     TRASH_EMAIL_DOMAINS = {
#         "sentry.io",
#         "example.com",
#         "example.org",
#         "example.net",
#         "mysite.com",
#         "yourdomain.com",
#         "domain.com",
#     }
#     TRASH_EMAIL_LOCAL_PARTS = {
#         "example", "test", "email", "user", "demo", "sample",
#         "dummy", "null", "none", "noreply", "no-reply",
#         "donotreply", "do-not-reply",
#         ".png", ".svg", ".jpg", ".jpeg", ".webp", ".gif",
#     }

#     custom_settings = {
#         "DEPTH_LIMIT": 10,
#         "CONCURRENT_REQUESTS": 16,
#         "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
#         "DOWNLOAD_TIMEOUT": 15,
#         "RETRY_ENABLED": False,
#         "ITEM_PIPELINES": {
#             "Nizami.pipelines.QualifiedSitesCsvPipeline": 300,
#         },
#         "QUALIFIED_SITES_OUTPUT": "output.csv",
#         "BRUTE_EMAIL_VALIDATE_DNS": False,
#     }

#     FOLLOW_URL_PATTERNS = [
#         "team", "our-team", "leadership", "executive", "founder",
#         "about", "about-us", "who-we-are", "management", "directors",
#         "people", "staff", "board", "c-suite",
#         "contact", "contact-us", "get-in-touch",
#         "company", "partners", "locations",
#         "support", "info", "email", "address",
#         "write-to-us", "inquiry", "enquiry", "hello"
#     ]

#     # ─── NEW semantic attributes ──────────────────────────────
#     LEADERSHIP_HINTS = [
#         "team", "people", "leadership", "governance", "management",
#         "executive", "board", "who-we-are", "about", "company",
#         "our-team", "founders", "directors", "c-suite", "staff",
#         "careers", "culture",            # added to catch team under /culture
#     ]

#     LEADERSHIP_TERMS = [
#         "our team", "meet our team", "leadership team", "executive committee",
#         "board of directors", "governance", "management team", "who we are",
#         "our people", "our leaders", "founder", "chief executive", "chief officer",
#         "executive team", "leadership", "directors", "management", "founders",
#     ]
#     # ─── OLD attributes (commented out) ──────────────────────
#     # LEADERSHIP_PATTERNS = [ ... ]   # removed
#     # LEADERSHIP_SEED_PATHS = [ ... ] # removed

#     AI_PAGE_TEXT_CAP = AI_MAX_TEXT_CHARS

#     @classmethod
#     def from_crawler(cls, crawler, *args, **kwargs):
#         spider = super().from_crawler(crawler, *args, **kwargs)
#         spider.crawler = crawler
#         crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
#         crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
#         return spider

#     def spider_closed(self, spider):
#         gc.collect()

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self.domain_phones = {}
#         self.domain_companies = {}
#         self.domain_websites = {}
#         self.domain_originals = {}
#         self.email_domain_dns_cache = {}
#         self.dns_resolver = dns.resolver.Resolver()
#         self.dns_resolver.timeout = 1.0
#         self.dns_resolver.lifetime = 2.0

#         self.include_keywords, self.block_keywords = self.load_keywords()
#         self.start_urls_list = self.load_input_files()

#         self.visited = set()
#         self.domain_data = {}
#         self.yielded_domains = set()
#         self.enriched_domains = set()
#         self.domain_ai_data = {}   # store latest AI result per domain

#         self.batch_size = 10
#         self.all_urls = self.start_urls_list
#         self.current_batch = -1

#         self.target = kwargs.get("target", "")
#         if not self.target:
#             self.target = getattr(self, "target", "")

#         code_nl = _get_code_nl_filter()
#         if code_nl is not None and code_nl.model is not None:
#             self.logger.info("CodeNL filter loaded from %s", code_nl.model_path)
#         elif not HAS_FASTTEXT:
#             self.logger.warning("fasttext not installed -- CodeNL DISABLED.")
#         else:
#             self.logger.warning("CodeNL model not found at %s -- CodeNL DISABLED.",
#                                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "code_nl_model.bin"))

#         if AI_ENABLED:
#             self.logger.info("AI ENABLED (%s providers).", len(AI_PROVIDER_NAMES))
#             if self.target:
#                 self.logger.info("Target: '%s'", self.target)
#         else:
#             self.logger.warning("g4f not installed -- AI DISABLED.")

#     # ─── NEW static methods ────────────────────────────────────────
#     @staticmethod
#     def leadership_score(url: str, text: str) -> int:
#         """Score a page by URL + content for leadership likelihood."""
#         score = 0
#         data = (url + " " + text).lower()
#         for term in FinderSpider.LEADERSHIP_TERMS:
#             if term in data:
#                 score += 10
#         for hint in FinderSpider.LEADERSHIP_HINTS:
#             if hint in url.lower():
#                 score += 5
#         return score

#     @staticmethod
#     def contains_people(text: str) -> bool:
#         """Detect if page actually contains executive/people signals."""
#         if not text:
#             return False
#         signals = [
#             "chief executive officer", "chief operating officer", "chief technology officer",
#             "chief financial officer", "chief marketing officer", "managing director",
#             "founder", "president", "director", "leadership team", "executive team",
#             "board member", "chairman", "ceo", "cto", "cfo", "cmo", "coo",
#         ]
#         return sum(1 for s in signals if s in text.lower()) >= 2

#     # ─── Sitemap parser, nav extractor ────────────────────────────
#     def _parse_sitemap(self, xml_text: str, base_url: str) -> List[str]:
#         if not xml_text:
#             return []
#         try:
#             root = ET.fromstring(xml_text)
#         except ET.ParseError:
#             return []
#         namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
#         sitemap_urls = []
#         for sm in root.findall('.//ns:loc', namespaces):
#             text = (sm.text or "").strip()
#             if text:
#                 sitemap_urls.append(text)
#         if not sitemap_urls:
#             sitemap_urls = [base_url]

#         leadership_urls = []
#         for url in sitemap_urls:
#             url_lower = url.lower()
#             if any(hint in url_lower for hint in self.LEADERSHIP_HINTS):
#                 leadership_urls.append(url)
#         return leadership_urls

#     def _extract_nav_links(self, response) -> List[str]:
#         links = set()
#         for nav in response.css("nav"):
#             for href in nav.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         for header in response.css("header"):
#             for href in header.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         for footer in response.css("footer"):
#             for href in footer.css("a::attr(href)").getall():
#                 links.add(response.urljoin(href))
#         domain = self.normalize_domain(response.url)
#         filtered = []
#         for url in links:
#             if domain not in urlparse(url).netloc.lower():
#                 continue
#             if any(hint in url.lower() for hint in self.LEADERSHIP_HINTS):
#                 filtered.append(url)
#         return filtered

#     # ─── NEW: JSON-LD structured data extraction (bypass LLM) ──
#     def _extract_jsonld_people(self, response, domain):
#         """Extract schema.org Person data directly — bypasses LLM entirely."""
#         people = []
#         for script in response.css('script[type="application/ld+json"]::text').getall():
#             try:
#                 data = json.loads(script)
#                 items = data if isinstance(data, list) else [data]
#                 for item in items:
#                     if item.get("@type") == "Person":
#                         name = item.get("name", "").strip()
#                         title = item.get("jobTitle", "").strip()
#                         if name and title:
#                             people.append((name, title))
#                     if item.get("@type") == "Organization":
#                         for emp in item.get("employee", []):
#                             if isinstance(emp, dict) and emp.get("@type") == "Person":
#                                 name = emp.get("name", "").strip()
#                                 title = emp.get("jobTitle", "").strip()
#                                 if name and title:
#                                     people.append((name, title))
#             except (json.JSONDecodeError, TypeError):
#                 continue

#         if people:
#             self.logger.info("JSON-LD: Found %s people on %s", len(people), response.url)
#             names = [p[0] for p in people]
#             titles = [p[1] for p in people]
#             self.domain_ai_data[domain] = {
#                 "person_name": ", ".join(names),
#                 "job_title": ", ".join(titles),
#                 "service": "Not Found",
#                 "relevancy": "True",
#                 "sales_hook": "Not Found",
#             }

#     # ─── Sitemap response handlers ──────────────────────────────
#     def parse_sitemap(self, response):
#         root_url = response.meta["root_url"]
#         domain = response.meta["domain"]
#         batch_index = response.meta["batch_index"]
#         leadership_urls = self._parse_sitemap(response.text, root_url)
#         if leadership_urls:
#             self.logger.info("SITEMAP: Found %s leadership URLs for %s", len(leadership_urls), domain)
#             for url in leadership_urls:
#                 if url not in self.visited:
#                     self.visited.add(url)
#                     yield scrapy.Request(
#                         url,
#                         callback=self.parse_page,
#                         errback=self.handle_request_error,
#                         priority=5,
#                         meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#                     )
#         else:
#             self.logger.info("SITEMAP: No leadership URLs found for %s, falling back to homepage", domain)
#             yield scrapy.Request(
#                 root_url,
#                 callback=self.parse_page,
#                 errback=self.handle_request_error,
#                 meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#             )

#     def _sitemap_error(self, failure):
#         root_url = failure.request.meta["root_url"]
#         domain = failure.request.meta["domain"]
#         batch_index = failure.request.meta["batch_index"]
#         self.logger.info("SITEMAP: Failed for %s, falling back to homepage", domain)
#         yield scrapy.Request(
#             root_url,
#             callback=self.parse_page,
#             errback=self.handle_request_error,
#             meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
#         )

#     # ─── Batch handling ──────────────────────────────────────────
#     def _start_batch(self, batch_index):
#         start = batch_index * self.batch_size
#         end = min(start + self.batch_size, len(self.all_urls))
#         batch_urls = self.all_urls[start:end]
#         if not batch_urls:
#             return
#         self.current_batch = batch_index
#         self.logger.info("Starting batch %s with %s URLs", batch_index + 1, len(batch_urls))
#         for url in batch_urls:
#             domain = self.normalize_domain(url)
#             parsed = urlparse(url)
#             sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
#             yield scrapy.Request(
#                 sitemap_url,
#                 callback=self.parse_sitemap,
#                 errback=self._sitemap_error,
#                 meta={"root_url": url, "domain": domain, "batch_index": batch_index},
#                 priority=10,
#             )

#     # def start_requests(self):
#     #     yield from self._start_batch(0)

#     def start_requests(self):
#         for url in self.all_urls:
#             domain = self.normalize_domain(url)
#             parsed = urlparse(url)
#             sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
#             yield scrapy.Request(
#                 sitemap_url,
#                 callback=self.parse_sitemap,
#                 errback=self._sitemap_error,
#                 meta={"root_url": url, "domain": domain, "batch_index": 0},
#                 priority=10,
#             )

#     def spider_idle(self, spider):
#         next_batch = self.current_batch + 1
#         if next_batch * self.batch_size >= len(self.all_urls):
#             return
#         self.logger.info("Spider idle, starting next batch %s", next_batch + 1)
#         for request in self._start_batch(next_batch):
#             try:
#                 self.crawler.engine.crawl(request)
#             except TypeError:
#                 self.crawler.engine.crawl(request, spider)
#         raise DontCloseSpider

#     # ─── Helper methods ──────────────────────────────────────────
#     @staticmethod
#     def normalize_domain(url):
#         return urlparse(url).netloc.lower().replace("www.", "").strip()

#     @staticmethod
#     def normalize_url(raw_url):
#         if pd.isna(raw_url):
#             return None
#         url = str(raw_url).strip()
#         if not url:
#             return None
#         if not url.startswith(("http://", "https://")):
#             url = f"http://{url}"
#         parsed = urlparse(url)
#         if not parsed.netloc or " " in parsed.netloc:
#             return None
#         return url

#     @classmethod
#     def is_trash_email(cls, email):
#         value = email.strip().lower()
#         if "@" not in value:
#             return True
#         local, domain = value.rsplit("@", 1)
#         domain = domain.lstrip(".")
#         if domain.startswith("www."):
#             domain = domain[4:]
#         if not local or not domain:
#             return True
#         if domain in cls.TRASH_EMAIL_DOMAINS or domain.startswith("example."):
#             return True
#         if domain.endswith(".sentry.io") or "mysite" in domain:
#             return True
#         if domain.endswith("wixpress.com"):
#             return True
#         if local in cls.TRASH_EMAIL_LOCAL_PARTS:
#             return True
#         if re.fullmatch(r"[0-9a-f]{24,}", local):
#             return True
#         if re.fullmatch(r"[0-9a-f-]{30,}", local):
#             return True
#         tld = domain.rsplit(".", 1)[-1]
#         if not re.fullmatch(r"[a-z]{2,10}", tld):
#             return True
#         if value.endswith(cls.IMAGE_SUFFIXES):
#             return True
#         return False

#     def email_domain_has_dns(self, domain):
#         key = domain.strip().lower()
#         if not key:
#             return False
#         cached = self.email_domain_dns_cache.get(key)
#         if cached is not None:
#             return cached
#         is_valid = False
#         try:
#             mx_records = self.dns_resolver.resolve(key, "MX")
#             is_valid = bool(mx_records)
#         except Exception:
#             try:
#                 a_records = self.dns_resolver.resolve(key, "A")
#                 is_valid = bool(a_records)
#             except Exception:
#                 is_valid = False
#         self.email_domain_dns_cache[key] = is_valid
#         return is_valid

#     def load_keywords(self):
#         project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
#         keyword_path = None
#         for file in os.listdir(project_root):
#             if file.lower() == "keywords.json":
#                 keyword_path = os.path.join(project_root, file)
#                 break
#         if not keyword_path:
#             raise FileNotFoundError(f"Keywords.json file not found in project root: {project_root}")
#         with open(keyword_path, "r", encoding="utf-8") as file:
#             data = json.load(file)
#         include_raw = data.get("include_keywords", [])
#         self.keyword_category = {}
#         if isinstance(include_raw, dict):
#             include = {}
#             for category, keywords in include_raw.items():
#                 category_keywords = [kw.lower() for kw in (keywords or [])]
#                 include[category] = category_keywords
#                 for kw in category_keywords:
#                     self.keyword_category[kw] = category
#         else:
#             include = {"General": [kw.lower() for kw in include_raw]}
#             self.keyword_category = {kw: "General" for kw in include["General"]}
#         block = [kw.lower() for kw in data.get("block_keywords", [])]
#         return include, block

#     def load_input_files(self):
#         input_folder = os.path.join(
#             os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Input"
#         )
#         urls = []
#         for file in sorted(os.listdir(input_folder)):
#             path = os.path.join(input_folder, file)
#             try:
#                 if file.endswith(".csv"):
#                     df = pd.read_csv(path, sep=None, engine="python", on_bad_lines="warn")
#                 elif file.endswith(".xlsx") or file.endswith(".xls"):
#                     df = pd.read_excel(path)
#                 else:
#                     continue
#             except Exception as error:
#                 self.logger.warning("Failed to read %s: %s", file, error)
#                 continue

#             url_column = None
#             phone_column = None
#             company_column = None
#             for col in df.columns:
#                 col_name = str(col).strip().lower()
#                 if col_name in ["website", "url", "website url"] and url_column is None:
#                     url_column = col
#                 if ("phone" in col_name or "contact" in col_name) and phone_column is None:
#                     phone_column = col
#                 if col_name in ["company name", "agency name", "name", "business name"] and company_column is None:
#                     company_column = col

#             if url_column is None:
#                 self.logger.warning("No URL column found in %s", file)
#                 continue

#             for _, row in df.iterrows():
#                 normalized_url = self.normalize_url(row[url_column])
#                 if not normalized_url:
#                     continue
#                 urls.append(normalized_url)
#                 domain = self.normalize_domain(normalized_url)

#                 if domain not in self.domain_originals:
#                     self.domain_originals[domain] = {}
#                     for col in df.columns:
#                         val = row.get(col)
#                         if pd.notna(val):
#                             self.domain_originals[domain][col] = str(val).strip()

#                 if domain not in self.domain_websites:
#                     self.domain_websites[domain] = normalized_url
#                 if phone_column is not None:
#                     phone = row[phone_column]
#                     if pd.notna(phone):
#                         phone_value = str(phone).strip()
#                         if phone_value and phone_value.lower() != "nan":
#                             self.domain_phones[domain] = phone_value
#                 if company_column is not None and domain not in self.domain_companies:
#                     company = row[company_column]
#                     if pd.notna(company):
#                         company_value = str(company).strip()
#                         if company_value and company_value.lower() != "nan":
#                             self.domain_companies[domain] = company_value
#         return list(set(urls))

#     @classmethod
#     def _decode_js_escapes(cls, source_text):
#         if not source_text:
#             return ""
#         decoded = cls.HEX_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), source_text,
#         )
#         decoded = cls.UNICODE_ESCAPE_REGEX.sub(
#             lambda match: chr(int(match.group(1), 16)), decoded,
#         )
#         return decoded

#     @classmethod
#     def _extract_emails_from_source(cls, source_text):
#         if not source_text:
#             return set()
#         blobs = []
#         raw = source_text
#         html_unescaped = unescape(raw)
#         url_decoded = unquote(html_unescaped)
#         js_decoded = cls._decode_js_escapes(url_decoded)
#         for blob in (raw, html_unescaped, url_decoded, js_decoded):
#             if blob and blob not in blobs:
#                 blobs.append(blob)
#         found = set()
#         for blob in blobs:
#             for email in cls.EMAIL_REGEX.findall(blob):
#                 found.add(email.strip().lower())
#             for email in cls.MAILTO_REGEX.findall(blob):
#                 normalized = unquote(email).strip().lower().replace("%40", "@")
#                 found.add(normalized)
#             for match in cls.OBFUSCATED_EMAIL_REGEX.finditer(blob):
#                 local = match.group(1).strip().lower()
#                 domain_part = match.group(2).strip().lower().strip(".")
#                 tld = match.group(3).strip().lower()
#                 found.add(f"{local}@{domain_part}.{tld}")
#         return found

#     def _build_email_candidates(self, source_text):
#         candidates = []
#         seen = set()
#         extracted = self._extract_emails_from_source(source_text)
#         for email in extracted:
#             normalized_email = email.strip().lower().replace("mailto:", "").strip(" <>\"'(),;")
#             if not normalized_email or normalized_email in seen:
#                 continue
#             if self.is_trash_email(normalized_email):
#                 continue
#             seen.add(normalized_email)
#             candidates.append(normalized_email)
#         return candidates

#     def _extract_socials(self, response, domain):
#         data = self.domain_data[domain]
#         for link in response.css("a::attr(href)").getall():
#             full_url = response.urljoin(link)
#             parsed = urlparse(full_url)
#             netloc = parsed.netloc.lower().replace("www.", "")
#             if not netloc:
#                 continue
#             matched = False
#             for platform, domains in self.SOCIAL_DOMAINS.items():
#                 if any(d in netloc for d in domains):
#                     data["socials"][platform].add(full_url)
#                     matched = True
#                     break
#             if not matched:
#                 if any(d in netloc for d in self.OTHER_SOCIAL_DOMAINS):
#                     data["socials"]["other"].add(full_url)

#     def _extract_tel_phones(self, source_text, domain):
#         data = self.domain_data[domain]
#         for match in self.TEL_HREF_REGEX.findall(source_text):
#             raw = match.strip()
#             cleaned = re.sub(r"[^\d+]", "", raw)
#             if cleaned:
#                 data["phones_scraped"].add(cleaned)

#     def handle_request_error(self, failure):
#         request = failure.request
#         self.logger.warning("Request failed: %s (%s)", request.url, failure.value)

#     # ─── NEW parse_page ────────────────────────────────────────────
#     async def parse_page(self, response):
#         root_url = response.meta["root_url"]
#         domain = self.normalize_domain(root_url)
#         batch_index = response.meta.get("batch_index", self.current_batch)
#         depth = response.meta.get("depth", 0)

#         outputs = []

#         try:
#             is_new_domain = domain not in self.domain_data
#             if is_new_domain:
#                 self.domain_data[domain] = {
#                     "include_count": 0,
#                     "matched_keywords": set(),
#                     "matched_categories": set(),
#                     "blocked": False,
#                     "emails": set(),
#                     "leadership_text": "",
#                     "general_text": "",
#                     "leadership_seeded": False,
#                     "socials": {
#                         "twitter": set(), "linkedin": set(), "facebook": set(),
#                         "instagram": set(), "youtube": set(), "other": set(),
#                     },
#                     "phones_scraped": set(),
#                 }

#             # ─── NEW: Structured data bypass ──────────────────────────
#             self._extract_jsonld_people(response, domain)

#             source_text = response.text
#             text = source_text.lower()

#             for keyword in self.block_keywords:
#                 if keyword in text:
#                     self.domain_data[domain]["blocked"] = True
#                     return outputs

#             all_keywords = [
#                 keyword
#                 for keywords in self.include_keywords.values()
#                 for keyword in keywords
#             ]

#             count = sum(text.count(keyword) for keyword in all_keywords)
#             self.domain_data[domain]["include_count"] += count

#             for keyword in all_keywords:
#                 if keyword in text:
#                     self.domain_data[domain]["matched_keywords"].add(keyword)
#                     category = self.keyword_category.get(keyword)
#                     if category:
#                         self.domain_data[domain]["matched_categories"].add(category)

#             # ─── Semantic page scoring ─────────────────────────────────
#             page_score = self.leadership_score(response.url, source_text)
#             has_people = self.contains_people(source_text)
#             is_leadership_page = page_score >= 15 or has_people

#             if is_leadership_page:
#                 self.logger.info("LEADERSHIP PAGE | score=%s | people=%s | %s", page_score, has_people, response.url)

#             if AI_ENABLED and len(self.domain_data[domain]["leadership_text"]) + len(self.domain_data[domain]["general_text"]) < self.AI_PAGE_TEXT_CAP:
#                 cleaned = _ai_clean_text(source_text)
#                 tagged = f"\n\n--- URL: {response.url} | TYPE: {'LEADERSHIP' if is_leadership_page else 'GENERAL'} | SCORE: {page_score} ---\n\n{cleaned}"
#                 if is_leadership_page:
#                     current = self.domain_data[domain]["leadership_text"]
#                     cap = self.AI_PAGE_TEXT_CAP // 2
#                     self.domain_data[domain]["leadership_text"] = (current + tagged)[:cap]
#                 else:
#                     current = self.domain_data[domain]["general_text"]
#                     cap = self.AI_PAGE_TEXT_CAP // 2
#                     self.domain_data[domain]["general_text"] = (current + tagged)[:cap]

#             # ─── NEW: Proactive leadership URL seeding ──────────────
#             if is_new_domain and not self.domain_data[domain].get("leadership_seeded"):
#                 self.domain_data[domain]["leadership_seeded"] = True
#                 base = response.url.rstrip("/")
#                 for path in [
#                     "about", "team", "leadership", "about-us", "meet-our-team",
#                     "who-we-are", "management", "our-team", "executive-team",
#                     "company/team", "company/about", "people", "staff",
#                     "directors", "board", "founders", "company/leadership",
#                     "company/people", "who-we-are/leadership", "about/team"
#                 ]:
#                     seed_url = f"{base}/{path}"
#                     if seed_url not in self.visited:
#                         self.visited.add(seed_url)
#                         outputs.append(scrapy.Request(
#                             seed_url,
#                             callback=self.parse_page,
#                             errback=self.handle_request_error,
#                             priority=300,
#                             meta={
#                                 "depth": depth + 1,
#                                 "root_url": root_url,
#                                 "batch_index": batch_index,
#                             },
#                         ))

#             self._extract_socials(response, domain)
#             self._extract_tel_phones(source_text, domain)

#             candidates = self._build_email_candidates(source_text)

#             results = []
#             if candidates:
#                 if self.settings.getbool("BRUTE_EMAIL_VALIDATE_DNS", False):
#                     deferreds = []
#                     for email in candidates:
#                         deferreds.append(threads.deferToThread(self._validate_email_dns, email))
#                     dlist = DeferredList(deferreds, consumeErrors=True)
#                     results = await maybe_deferred_to_future(dlist)
#                 else:
#                     results = [(True, email) for email in candidates]

#             more_outputs = await self._after_dns_checks(results, response, domain, batch_index, depth, page_score)
#             outputs.extend(more_outputs)

#             # Intelligent link discovery — score all internal links
#             if depth < 10:
#                 follow_requests = self._get_follow_requests(response, domain, batch_index, depth, page_score)
#                 outputs.extend(follow_requests)

#         finally:
#             self.logger.debug("Request finished: %s", response.url)

#         return outputs

#     # ───── OLD parse_page (commented out) ──────────────────────────
#     """
#     async def parse_page(self, response):
#         ... old implementation ...
#     """

#     def _validate_email_dns(self, email):
#         email_domain = email.rsplit("@", 1)[-1]
#         if self.email_domain_has_dns(email_domain):
#             return email
#         return None

#     # ─── NEW _after_dns_checks (delayed yield) ────────────────────
#     async def _after_dns_checks(self, results, response, domain, batch_index, depth, page_score=0):
#         for success, email_or_none in results:
#             if success and email_or_none:
#                 self.domain_data[domain]["emails"].add(email_or_none)

#         outputs = []
#         data = self.domain_data[domain]

#         if data["blocked"] or data["include_count"] < 2:
#             if depth < 10:
#                 outputs.extend(self._get_follow_requests(response, domain, batch_index, depth, page_score))
#             return outputs

#         # ─── NEW: Delay yield until leadership text exists or deep enough ──
#         has_leadership_text = len(data["leadership_text"]) > 200
#         deep_enough = depth >= 2

#         if domain not in self.yielded_domains and (has_leadership_text or deep_enough):
#             self.yielded_domains.add(domain)
#             item = await self._enrich_and_build_item(domain)
#             outputs.append(item)

#         if depth < 10:
#             outputs.extend(self._get_follow_requests(response, domain, batch_index, depth, page_score))

#         return outputs

#     # ───── OLD _after_dns_checks (commented out) ──────────────
#     """
#     async def _after_dns_checks(self, results, response, domain, batch_index, depth):
#         ... old implementation ...
#     """

#     # ─── NEW _get_follow_requests (adaptive scoring) ──────────────
#     def _get_follow_requests(self, response, domain, batch_index, depth, current_page_score=0):
#         """Score every internal link and crawl high-potential pages deeper."""
#         root_url = response.meta["root_url"]
#         links = response.css("a::attr(href)").getall()

#         requests = []
#         seen = set()

#         for link in links:
#             next_url = response.urljoin(link)

#             if domain not in urlparse(next_url).netloc.lower():
#                 continue
#             if next_url.lower().endswith((".jpg", ".png", ".pdf", ".zip", ".gif", ".svg", ".webp")):
#                 continue
#             if next_url in seen or next_url in self.visited:
#                 continue
#             seen.add(next_url)

#             # Score link by URL hints
#             link_score = 0
#             url_lower = next_url.lower()
#             for hint in self.LEADERSHIP_HINTS:
#                 if hint in url_lower:
#                     link_score += 10

#             # Only follow links that look promising OR we're on a leadership page
#             if link_score == 0 and current_page_score < 15:
#                 continue

#             self.visited.add(next_url)

#             # Adaptive depth: leadership branches get deeper crawling
#             if current_page_score >= 15:
#                 next_depth = depth  # free depth on leadership branches
#             else:
#                 next_depth = depth + 1

#             requests.append(scrapy.Request(
#                 next_url,
#                 callback=self.parse_page,
#                 errback=self.handle_request_error,
#                 priority=link_score + current_page_score,
#                 meta={
#                     "depth": next_depth,
#                     "root_url": root_url,
#                     "batch_index": batch_index,
#                 },
#             ))

#         return requests

#     # ───── OLD _get_follow_requests (commented out) ──────────────
#     """
#     def _get_follow_requests(self, response, domain, batch_index, depth):
#         ... old implementation ...
#     """

#     # ─── NEW _enrich_and_build_item (always runs, stores result) ──
#     async def _enrich_and_build_item(self, domain):
#         """Run AI enrichment and build the item."""
#         if not AI_ENABLED:
#             return self.build_item(domain, {})

#         company_name = self.domain_companies.get(domain, "")
#         leadership_text = self.domain_data[domain]["leadership_text"]
#         general_text = self.domain_data[domain]["general_text"]

#         self.logger.info("Running AI lead extraction for domain: %s", domain)
#         ai_data = await maybe_deferred_to_future(
#             threads.deferToThread(
#                 ai_get_lead_info, domain, company_name,
#                 leadership_text, general_text, self.target
#             )
#         )
#         gc.collect()

#         self.domain_ai_data[domain] = ai_data or {}
#         return self.build_item(domain, ai_data)

#     # ───── OLD _enrich_and_build_item (commented out) ─────────────
#     """
#     async def _enrich_and_build_item(self, domain):
#         ... old implementation ...
#     """

#     def build_item(self, domain, ai_data=None):
#         data = self.domain_data[domain]
#         original = self.domain_originals.get(domain, {})
#         # Use passed ai_data, or fallback to stored, or empty
#         ai_data = ai_data or self.domain_ai_data.get(domain, {}) or {}

#         def orig(field, default=""):
#             val = original.get(field, "")
#             return val if val and val.lower() not in ("nan",) else default

#         def merge(field, ai_key=None, scraped=""):
#             o = orig(field)
#             if o and o not in ("Not Found", "False"):
#                 return o
#             if ai_key and ai_data.get(ai_key) and ai_data[ai_key] not in ("Not Found", "False"):
#                 return ai_data[ai_key]
#             if scraped:
#                 return scraped
#             return "Not Found"

#         orig_rel = orig("Relevancy")
#         if orig_rel and orig_rel not in ("Not Found", "False"):
#             relevancy = orig_rel
#         else:
#             relevancy = ai_data.get("relevancy", "False")
#         socials = data["socials"]

#         item = {
#             "Website URL": domain,
#             "Company Name": orig("Company Name", self.domain_companies.get(domain, "")),
#             "Phone Number": orig("Phone Number", self.domain_phones.get(domain, "")),
#             "Additional Phone Numbers": ", ".join(sorted(data["phones_scraped"])) if data["phones_scraped"] else "",
#             "Person Name": merge("Person Name", "person_name"),
#             "Job Title": merge("Job Title", "job_title"),
#             "Relevancy": relevancy,
#             "Service": merge("Service", "service"),
#             "Sales Hook": ai_data.get("sales_hook", "Not Found"),
#             "Keywords - Team": orig("Keywords - Team", ",".join(sorted(data["matched_keywords"]))),
#             "Specification": orig("Specification", ",".join(sorted(data["matched_categories"]))),
#             "Emails": orig("Emails", ",".join(sorted(data["emails"]))),
#             "Twitter": ", ".join(sorted(socials["twitter"])),
#             "LinkedIn": ", ".join(sorted(socials["linkedin"])),
#             "Facebook": ", ".join(sorted(socials["facebook"])),
#             "Instagram": ", ".join(sorted(socials["instagram"])),
#             "YouTube": ", ".join(sorted(socials["youtube"])),
#             "Other Social": ", ".join(sorted(socials["other"])),
#         }

#         self.logger.info(
#             "YIELDING ITEM for domain: %s | Person: %s | Job: %s | Service: %s | Relevancy: %s | Hook: %s",
#             domain,
#             item["Person Name"], item["Job Title"], item["Service"],
#             item["Relevancy"], item["Sales Hook"],
#         )
#         return item









import os
os.environ["SE_HEADLESS"] = "1"
os.environ["PLAYWRIGHT_HEADLESS"] = "1"
os.environ["CHROME_HEADLESS"] = "1"
os.environ["MOZ_HEADLESS"] = "1"
os.environ["PYPPETEER_HEADLESS"] = "1"
os.environ["HEADLESS"] = "1"
import gc
import json
import random
import re
import time
import warnings
import concurrent.futures
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse, urljoin, urldefrag
from xml.etree import ElementTree as ET

import dns.resolver
import pandas as pd
import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.utils.defer import maybe_deferred_to_future
from twisted.internet import threads
from twisted.internet.defer import DeferredList

try:
    import g4f
    from g4f import Provider
    from g4f.client import Client
    HAS_G4F = True
except ImportError:
    HAS_G4F = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

try:
    import fasttext
    HAS_FASTTEXT = True
except ImportError:
    HAS_FASTTEXT = False

warnings.filterwarnings("ignore", message="Unclosed client session")
warnings.filterwarnings("ignore", message="Unclosed connector")

AI_ENABLED = HAS_G4F
AI_PROVIDER_NAMES = [
    "Cloudflare",
    "CohereForAI_C4AI_Command",
    "AnyProvider",
    "Gemini",
    "Perplexity",
    "Yqcloud",
    "OpenaiChat",
    "MiniMax",
    "WhiteRabbitNeo",
]
AI_FALLBACK_MODEL = "gpt-4o-mini"
AI_PER_CALL_TIMEOUT = 35
AI_RACE_TIMEOUT = 50
AI_MAX_RETRIES = 5
AI_RETRY_BASE_DELAY = 2.0
AI_MAX_WORKERS = min(12, len(AI_PROVIDER_NAMES))
AI_MAX_TEXT_CHARS = 15000
AI_CHUNK_MIN_LENGTH = 50
AI_CHUNK_MAX_LENGTH = 1200
AI_CODENL_THRESHOLD = 0.85
AI_REQUIRED_KEYS = {"person_name", "job_title", "service", "relevancy", "sales_hook"}

class CodeNLFilter:
    def __init__(self, model_path: str, threshold: float = AI_CODENL_THRESHOLD):
        self.model_path = model_path
        self.threshold = threshold
        self.model: Optional[fasttext.FastText] = None
        self._load_model()

    def _load_model(self) -> None:
        if not HAS_FASTTEXT:
            return
        if not os.path.exists(self.model_path):
            return
        try:
            self.model = fasttext.load_model(self.model_path)
        except Exception:
            self.model = None

    def is_code(self, text: str) -> Tuple[bool, float]:
        if self.model is None or not text or len(text) < 10:
            return False, 0.0
        try:
            labels, probs = self.model.predict(text.replace("\n", " "), k=1)
            label = labels[0].replace("__label__", "")
            confidence = float(probs[0])
            is_code = (label.lower() == "code" and confidence >= self.threshold)
            return is_code, confidence
        except Exception:
            return False, 0.0

    def filter_text(self, text: str) -> str:
        if self.model is None or not text:
            return text
        chunks = self._semantic_chunk(text)
        clean_chunks: List[str] = []
        for chunk in chunks:
            is_code, _ = self.is_code(chunk)
            if not is_code:
                clean_chunks.append(chunk)
        return "\n".join(clean_chunks)

    @staticmethod
    def _semantic_chunk(text: str) -> List[str]:
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        chunks: List[str] = []
        for para in paragraphs:
            if len(para) < AI_CHUNK_MIN_LENGTH:
                continue
            if len(para) <= AI_CHUNK_MAX_LENGTH:
                chunks.append(para)
            else:
                sentences = re.split(r'(?<=[.!?])\s+', para)
                current = ""
                for sent in sentences:
                    sent = sent.strip()
                    if not sent:
                        continue
                    if len(current) + len(sent) + 1 <= AI_CHUNK_MAX_LENGTH:
                        current = (current + " " + sent).strip() if current else sent
                    else:
                        if current and len(current) >= AI_CHUNK_MIN_LENGTH:
                            chunks.append(current)
                        current = sent
                if current and len(current) >= AI_CHUNK_MIN_LENGTH:
                    chunks.append(current)
        return chunks

_code_nl_filter: Optional[CodeNLFilter] = None

def _get_code_nl_filter() -> Optional[CodeNLFilter]:
    global _code_nl_filter
    if _code_nl_filter is not None:
        return _code_nl_filter
    if not HAS_FASTTEXT:
        return None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(script_dir, "code_nl_model.bin")
    _code_nl_filter = CodeNLFilter(model_path, AI_CODENL_THRESHOLD)
    return _code_nl_filter

# ============================================================
# _ai_clean_text (unchanged)
# ============================================================
def _ai_clean_text(html_text: str) -> str:
    if not html_text:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html_text, "lxml")
        for tag in soup(["script", "style", "noscript", "svg", "iframe", "nav", "footer"]):
            tag.decompose()

        for tag in soup.find_all(True):
            if not hasattr(tag, "name") or tag.name is None:
                continue
            if getattr(tag, "attrs", None) is None:
                continue
            try:
                cls = " ".join(tag.get("class") or []).lower()
                id_attr = (tag.get("id") or "").lower()
            except Exception:
                continue
            if any(kw in cls or kw in id_attr for kw in [
                "testimonial", "case-study", "case_study", "review", "quote",
                "customer-story", "success-story", "client-logo", "partner-logo",
                "client-list", "partner-list", "trusted-by", "used-by",
            ]):
                tag.decompose()

        text = soup.get_text(separator=" ", strip=True)
    else:
        text = re.sub(r"<[^>]+>", " ", html_text)
    text = re.sub(r"\s+", " ", text).strip()

    code_nl = _get_code_nl_filter()
    if code_nl is not None and code_nl.model is not None:
        text = code_nl.filter_text(text)

    return text

# ============================================================
# _ai_build_prompt – UPDATED to force "all people"
# ============================================================
def _ai_build_prompt(domain: str, company_name: str, leadership_text: str,
                     general_text: str, target: str) -> str:
    target_str = target if target else "general business relevance"
    example = (
        '{"people":['  
        '{"person_name":"Alex Smith","job_title":"Founder & CEO","context":"Alex Smith, Founder & CEO, leads our team"},'
        '{"person_name":"Max Doe","job_title":"CTO","context":"Max Doe serves as CTO"},'
        '{"person_name":"Sarah Lee","job_title":"VP of Engineering","context":"Sarah Lee, VP of Engineering, manages our dev teams"},'
        '{"person_name":"James Brown","job_title":"Director of Marketing","context":"James Brown is the Director of Marketing"}'
        '],"service":"AI recruitment software","relevancy":true,'
        '"sales_hook":"What is the biggest bottleneck in scaling your AI recruitment platform?"}'
    )

    parts = [
        "You are a strict B2B lead researcher. Extract ONLY current employees of THIS company.",
        "",
        "=== ABSOLUTE RULES — VIOLATING ANY OF THESE IS A FAILURE ===",
        "1. people: JSON array of objects. Each object MUST have:",
        "   - person_name: FULL NAME only. No company names, no quotes, no descriptors.",
        "   - job_title: Their EXACT internal job title at THIS company ONLY.",
        "   - context: The EXACT sentence/phrase from the text where this person appears with their title.",
        "",
        "2. ⚠️ YOU MUST EXTRACT EVERY PERSON MENTIONED — NOT JUST THE MOST SENIOR.",
        "   - Include ALL founders, executives, directors, managers, and any employee listed.",
        "   - If you see 5 people, return all 5. If you see 20, return all 20.",
        "   - Do NOT stop after finding the CEO – the job is to list everyone.",
        "",
        "3. REJECT and NEVER extract any of the following:",
        "   - Customer testimonial givers (e.g. 'Jane Doe, CMO at ClientCorp')",
        "   - Case study subjects or featured clients",
        "   - Partner company employees or partner logos",
        "   - Advisory board members, board of directors, or external advisors UNLESS they are full-time employees",
        "   - Investors, VCs, or funding partners",
        "   - Former employees, ex-employees, or alumni",
        "   - Anyone whose title mentions another company name (e.g. 'CMO, Vercel' or 'Manager at Zendesk')",
        "   - Anyone appearing in a quote, review, or testimonial section",
        "   - Generic placeholders like 'John Doe' or 'Jane Smith'",
        "",
        "4. A person MUST be a current internal employee of the company being scraped.",
        "   - Titles like 'CMO, Vercel' or 'Director at Google' = EXTERNAL → REJECT",
        "   - Titles like 'Our client, Bob from Acme' = EXTERNAL → REJECT",
        "   - Titles like 'Former CTO' or 'Ex-CEO' = EXTERNAL → REJECT",
        "   - Titles with '@' and another company name (e.g. 'CEO @ MealBox') = EXTERNAL → REJECT",
        "",
        "5. If you are unsure whether a person is an employee, REJECT them.",
        "",
        "6. service: What this company sells/does in 2-8 words. Be specific.",
        "",
        '7. relevancy: true or false. Does this company match: "' + target_str + '"?',
        "   - true = clearly fits. false = does not fit or unclear.",
        "",
        "8. sales_hook: ONE concise first question (max 12 words) for the MOST SENIOR employee.",
        "   - This is only for the top leader; it does NOT affect the extraction of all other people.",
        "",
        "Respond with ONLY a single-line valid JSON object. No markdown, no explanation.",
        "",
        "Example: " + example,
        "",
        "=== LEADERSHIP / TEAM PAGES (highest priority — extract from these first) ===",
        '"""' + (leadership_text[:8000] if leadership_text else "(no leadership text scraped)") + '"""',
        "",
        "=== GENERAL SITE PAGES (lower priority — use only if leadership pages are empty) ===",
        '"""' + (general_text[:6000] if general_text else "(no general text scraped)") + '"""',
    ]
    return "\n".join(parts)

# ============================================================
# _ai_parse_json (unchanged – already has post-validation)
# ============================================================
def _ai_parse_json(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    raw = raw.strip()
    raw = re.sub(r"^```(json)?", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"```$", "", raw).strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    # ── POST-VALIDATION: reject testimonial / customer / partner names ──
    REJECT_TITLE_KEYWORDS = {
        " at ", " of ", " from ", "formerly", "ex-", "ex ", "former ", "client",
        "customer", "partner", "partnered", "testimonial", "advisory", "investor",
        "board member", "external", "alumni", "retired",
    }
    REJECT_CONTEXT_KEYWORDS = {
        "testimonial", "client", "customer", "partner", "partnered with",
        "case study", "our client", "our customer", "success story",
        "featured", "review", "quote from", "says ", "said ", "trusted by",
    }
    PLACEHOLDER_NAMES = {"john doe", "jane smith", "jane doe", "john smith", "placeholder"}

    result = {}

    people_list = data.get("people", [])
    if isinstance(people_list, list) and people_list:
        names = []
        titles = []
        for p in people_list:
            if not isinstance(p, dict):
                continue

            pname = str(p.get("person_name", "")).strip()
            ptitle = str(p.get("job_title", "")).strip()
            pcontext = str(p.get("context", "")).strip().lower()

            if not pname or not ptitle:
                continue
            if pname.lower() in PLACEHOLDER_NAMES:
                continue
            if ptitle.lower() in ("null", "none", "n/a", "unknown", "not found"):
                continue

            title_lower = ptitle.lower()
            if any(kw in title_lower for kw in REJECT_TITLE_KEYWORDS):
                continue
            if any(kw in pcontext for kw in REJECT_CONTEXT_KEYWORDS):
                continue
            if re.search(r",\s*[A-Z][a-zA-Z]+", ptitle):
                continue
            if re.search(r"@\s*[A-Z]", ptitle):
                continue

            names.append(pname)
            titles.append(ptitle)

        result["person_name"] = ", ".join(names) if names else "Not Found"
        result["job_title"] = ", ".join(titles) if titles else "Not Found"
    else:
        for key in ("person_name", "job_title"):
            val = str(data.get(key, "")).strip()
            if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
                result[key] = "Not Found"
            else:
                result[key] = val

    for key in ("service", "sales_hook"):
        val = str(data.get(key, "")).strip()
        if not val or val.lower() in ("null", "none", "n/a", "unknown", "not found"):
            result[key] = "Not Found"
        else:
            result[key] = val

    rel = str(data.get("relevancy", "")).strip().lower()
    result["relevancy"] = "True" if rel in ("true", "yes", "1", "high") else "False"

    if not AI_REQUIRED_KEYS.issubset(result.keys()):
        return None

    return result

def _ai_get_provider_class(name: str):
    return getattr(Provider, name, None)

def _ai_pick_model(provider_class) -> str:
    models = getattr(provider_class, "models", None)
    return models[0] if models else AI_FALLBACK_MODEL

def _ai_call_provider(provider_name: str, prompt: str) -> Optional[Dict[str, Any]]:
    provider_class = _ai_get_provider_class(provider_name)
    if provider_class is None:
        return None
    model = _ai_pick_model(provider_class)
    client = Client()
    try:
        response = client.chat.completions.create(
            model=model,
            provider=provider_class,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            timeout=AI_PER_CALL_TIMEOUT,
        )
        content = None
        if getattr(response, "choices", None):
            message = response.choices[0].message
            content = getattr(message, "content", None)
        if not isinstance(content, str):
            return None
        return _ai_parse_json(content)
    except Exception:
        return None
    finally:
        del client

def _ai_race_providers(prompt: str) -> Optional[Dict[str, Any]]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as executor:
        future_to_name = {
            executor.submit(_ai_call_provider, name, prompt): name
            for name in AI_PROVIDER_NAMES
        }
        winner = None
        try:
            for future in concurrent.futures.as_completed(future_to_name, timeout=AI_RACE_TIMEOUT):
                try:
                    result = future.result()
                except Exception:
                    result = None
                if result is not None:
                    winner = result
                    break
        except concurrent.futures.TimeoutError:
            pass
        executor.shutdown(wait=False, cancel_futures=True)
    return winner

def ai_get_lead_info(domain: str, company_name: str, leadership_text: str,
                     general_text: str, target: str) -> Dict[str, str]:
    if not AI_ENABLED:
        return {
            "person_name": "Not Found", "job_title": "Not Found",
            "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
        }
    prompt = _ai_build_prompt(domain, company_name, leadership_text, general_text, target)
    for attempt in range(1, AI_MAX_RETRIES + 1):
        result = _ai_race_providers(prompt)
        if result is not None:
            return result
        delay = AI_RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
        time.sleep(min(delay, 20))
        gc.collect()
    return {
        "person_name": "Not Found", "job_title": "Not Found",
        "service": "Not Found", "relevancy": "False", "sales_hook": "Not Found"
    }

# ============================================================
# SPIDER
# ============================================================

class FinderSpider(scrapy.Spider):
    name = "Gliner_Spider"

    # ─── Existing constants ────────────────────────────────────
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
    TEL_HREF_REGEX = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)

    SOCIAL_DOMAINS = {
        "twitter": ["twitter.com", "x.com"],
        "linkedin": ["linkedin.com"],
        "facebook": ["facebook.com", "fb.com"],
        "instagram": ["instagram.com"],
        "youtube": ["youtube.com", "youtu.be"],
    }
    OTHER_SOCIAL_DOMAINS = [
        "tiktok.com", "pinterest.com", "reddit.com", "github.com",
        "medium.com", "snapchat.com", "discord.com", "discord.gg",
        "t.me", "telegram.me", "whatsapp.com", "wa.me",
    ]

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
        "example", "test", "email", "user", "demo", "sample",
        "dummy", "null", "none", "noreply", "no-reply",
        "donotreply", "do-not-reply",
        ".png", ".svg", ".jpg", ".jpeg", ".webp", ".gif",
    }

    custom_settings = {
        "DEPTH_LIMIT": 10,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_ENABLED": False,
        "ITEM_PIPELINES": {
            "Nizami.pipelines.QualifiedSitesCsvPipeline": 300,
        },
        "QUALIFIED_SITES_OUTPUT": "output.csv",
        "BRUTE_EMAIL_VALIDATE_DNS": False,
    }

    FOLLOW_URL_PATTERNS = [
        "team", "our-team", "leadership", "executive", "founder",
        "about", "about-us", "who-we-are", "management", "directors",
        "people", "staff", "board", "c-suite",
        "contact", "contact-us", "get-in-touch",
        "company", "partners", "locations",
        "support", "info", "email", "address",
        "write-to-us", "inquiry", "enquiry", "hello"
    ]

    # ─── Discovery tuning constants (keyword-first) ──────────
    SCORE_INCLUDE_URL       = 30
    SCORE_INCLUDE_ANCHOR    = 25
    SCORE_LEADERSHIP_URL    = 15
    SCORE_LEADERSHIP_ANCHOR = 10

    SCORE_DEPTH_PENALTY     = -3
    SCORE_QUERY_PENALTY     = -5
    SCORE_BLOCK_KILL        = -999

    SCORE_KEYWORD_MATCHED   = 25
    SCORE_GENERAL_MATCHED   = 15

    SKIP_EXTENSIONS = {
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
        ".pdf", ".zip", ".tar", ".gz", ".mp4", ".mp3", ".mov",
        ".css", ".js", ".xml", ".json", ".rss", ".atom",
    }

    MAX_LINKS_PER_PAGE = 40

    LEADERSHIP_HINTS = [
        "team", "people", "leadership", "governance", "management",
        "executive", "board", "who-we-are", "about", "company",
        "our-team", "founders", "directors", "c-suite", "staff",
        "careers", "culture", "know-us", "meet-the-team", "board-of-directors",
        "our-people", "executive-team", "management-team", "leadership-team",
        "about-us", "meet-our-team", "the-team", "company-team", "who-we-are",
    ]

    AI_PAGE_TEXT_CAP = AI_MAX_TEXT_CHARS

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.crawler = crawler
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        gc.collect()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.domain_phones = {}
        self.domain_companies = {}
        self.domain_websites = {}
        self.domain_originals = {}
        self.email_domain_dns_cache = {}
        self.dns_resolver = dns.resolver.Resolver()
        self.dns_resolver.timeout = 1.0
        self.dns_resolver.lifetime = 2.0

        self.include_keywords, self.block_keywords = self.load_keywords()
        self.start_urls_list = self.load_input_files()

        self.visited = set()
        self.domain_data = {}
        self.yielded_domains = set()
        self.enriched_domains = set()
        self.domain_ai_data = {}

        self.batch_size = 10
        self.all_urls = self.start_urls_list
        self.current_batch = -1

        self.target = kwargs.get("target", "")
        if not self.target:
            self.target = getattr(self, "target", "")

        code_nl = _get_code_nl_filter()
        if code_nl is not None and code_nl.model is not None:
            self.logger.info("CodeNL filter loaded from %s", code_nl.model_path)
        elif not HAS_FASTTEXT:
            self.logger.warning("fasttext not installed -- CodeNL DISABLED.")
        else:
            self.logger.warning("CodeNL model not found at %s -- CodeNL DISABLED.",
                                os.path.join(os.path.dirname(os.path.abspath(__file__)), "code_nl_model.bin"))

        if AI_ENABLED:
            self.logger.info("AI ENABLED (%s providers).", len(AI_PROVIDER_NAMES))
            if self.target:
                self.logger.info("Target: '%s'", self.target)
        else:
            self.logger.warning("g4f not installed -- AI DISABLED.")

    # ─── Normalize URL for deduping ──────────────────────────
    @staticmethod
    def _normalize_for_dedup(url: str) -> str:
        url = url.split("#")[0]
        url = url.rstrip("/")
        url = re.sub(r"^https?://(www\.)?", "", url, flags=re.IGNORECASE)
        return url.lower()

    # ─── Helper: check if URL contains any include keyword ──
    def _url_has_keyword_match(self, url: str) -> bool:
        """Return True if the URL path contains any include_keyword."""
        parsed = urlparse(url)
        path = unquote(parsed.path).lower()
        for category, keywords in self.include_keywords.items():
            for kw in keywords:
                if kw in path:
                    return True
        return False

    # ─── Unified link scorer (keyword-first) ─────────────────
    def _score_link(
        self,
        url: str,
        anchor_text: str,
        domain: str,
        current_depth: int,
    ) -> Tuple[int, bool]:
        parsed = urlparse(url)
        path = unquote(parsed.path).lower()
        query = parsed.query.lower()
        anchor = anchor_text.lower().strip()
        netloc = parsed.netloc.lower().replace("www.", "")

        if domain not in netloc:
            return 0, False

        if any(path.endswith(ext) for ext in self.SKIP_EXTENSIONS):
            return 0, False

        if not path and not query:
            return 0, False

        # Block any URL with blog in path
        if "/blog" in path or "/blogs" in path:
            return self.SCORE_BLOCK_KILL, False

        score = 0
        path_parts = [p for p in path.split("/") if p]

        # KEYWORD matches (PRIMARY)
        for category, keywords in self.include_keywords.items():
            for kw in keywords:
                if kw in path:
                    score += self.SCORE_INCLUDE_URL

        for kw in self.block_keywords:
            if kw in path:
                return self.SCORE_BLOCK_KILL, False

        # Leadership hints (SECONDARY)
        for hint in self.LEADERSHIP_HINTS:
            if hint in path:
                score += self.SCORE_LEADERSHIP_URL

        # Anchor text matches
        for category, keywords in self.include_keywords.items():
            for kw in keywords:
                if kw in anchor:
                    score += self.SCORE_INCLUDE_ANCHOR

        for kw in self.block_keywords:
            if kw in anchor:
                return self.SCORE_BLOCK_KILL, False

        for hint in self.LEADERSHIP_HINTS:
            if hint in anchor:
                score += self.SCORE_LEADERSHIP_ANCHOR

        depth_penalty = max(0, len(path_parts) - 2) * self.SCORE_DEPTH_PENALTY
        score += depth_penalty

        if query:
            score += self.SCORE_QUERY_PENALTY

        if len(path_parts) <= 2 and score > 0:
            score += 5

        if domain in self.domain_data:
            if self.domain_data[domain]["include_count"] > 0:
                score += 8
            if self.domain_data[domain].get("leadership_text"):
                score += 5

        return score, True

    # ─── Recursive sitemap parser ────────────────────────────
    def _parse_sitemap(self, xml_text: str, base_url: str) -> Tuple[List[str], List[str]]:
        if not xml_text:
            return [], []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return [], []

        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        page_urls = []
        nested_sitemaps = []

        sitemaps = root.findall(".//ns:sitemap/ns:loc", ns)
        urls = root.findall(".//ns:url/ns:loc", ns)

        for loc in sitemaps:
            text = (loc.text or "").strip()
            if text:
                nested_sitemaps.append(text)

        for loc in urls:
            text = (loc.text or "").strip()
            if text:
                page_urls.append(text)

        return page_urls, nested_sitemaps

    # ─── Sitemap handler with two-pass scoring ───────────────
    def parse_sitemap(self, response):
        root_url = response.meta["root_url"]
        domain = response.meta["domain"]
        batch_index = response.meta["batch_index"]

        page_urls, nested_sitemaps = self._parse_sitemap(response.text, root_url)

        # Follow nested sitemaps
        for sitemap_url in nested_sitemaps:
            norm = self._normalize_for_dedup(sitemap_url)
            if norm not in self.visited:
                self.visited.add(norm)
                yield scrapy.Request(
                    sitemap_url,
                    callback=self.parse_sitemap,
                    errback=self._sitemap_error,
                    priority=10,
                    meta={"root_url": root_url, "domain": domain, "batch_index": batch_index},
                )

        # Score every URL
        scored_urls = []
        for url in page_urls:
            score, ok = self._score_link(url, "", domain, 0)
            if ok:
                scored_urls.append((score, url))

        scored_urls.sort(key=lambda x: x[0], reverse=True)

        keyword_urls = [url for score, url in scored_urls if score >= self.SCORE_KEYWORD_MATCHED]
        leadership_urls = [url for score, url in scored_urls
                           if self.SCORE_GENERAL_MATCHED <= score < self.SCORE_KEYWORD_MATCHED]

        if keyword_urls:
            self.logger.info(
                "SITEMAP-KEYWORDS: %s keyword-matched URLs for %s (top: %s)",
                len(keyword_urls), domain, scored_urls[0][0] if scored_urls else 0,
            )
            for url in keyword_urls[:30]:
                norm = self._normalize_for_dedup(url)
                if norm not in self.visited:
                    self.visited.add(norm)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        priority=5,
                        meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
                    )

        elif leadership_urls:
            self.logger.info(
                "SITEMAP-HINTS: No keyword matches for %s, falling back to %s leadership URLs",
                domain, len(leadership_urls),
            )
            for url in leadership_urls[:25]:
                norm = self._normalize_for_dedup(url)
                if norm not in self.visited:
                    self.visited.add(norm)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        priority=4,
                        meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
                    )

        else:
            self.logger.info("SITEMAP: No matches for %s, falling back to homepage", domain)
            yield scrapy.Request(
                root_url,
                callback=self.parse_homepage_for_links,
                errback=self._homepage_error,
                meta={"root_url": root_url, "domain": domain, "batch_index": batch_index},
            )

    def _sitemap_error(self, failure):
        root_url = failure.request.meta["root_url"]
        domain = failure.request.meta["domain"]
        batch_index = failure.request.meta["batch_index"]
        self.logger.info("SITEMAP: Failed for %s, falling back to homepage", domain)
        yield scrapy.Request(
            root_url,
            callback=self.parse_homepage_for_links,
            errback=self._homepage_error,
            meta={"root_url": root_url, "domain": domain, "batch_index": batch_index},
        )

    # ─── Homepage link discovery with two-pass scoring ──────
    def parse_homepage_for_links(self, response):
        root_url = response.meta["root_url"]
        domain = response.meta["domain"]
        batch_index = response.meta["batch_index"]

        scored_links = []
        seen_on_page = set()

        for link in response.css("a"):
            href = link.css("::attr(href)").get("")
            anchor_text = link.css("::text").get("") or ""
            anchor_text = re.sub(r"\s+", " ", anchor_text).strip()

            full_url = response.urljoin(href)
            norm = self._normalize_for_dedup(full_url)

            if norm in seen_on_page:
                continue
            seen_on_page.add(norm)

            score, ok = self._score_link(full_url, anchor_text, domain, 0)
            if ok:
                scored_links.append((score, full_url))

        scored_links.sort(key=lambda x: x[0], reverse=True)

        keyword_links = [url for score, url in scored_links if score >= self.SCORE_KEYWORD_MATCHED]
        leadership_links = [url for score, url in scored_links
                            if self.SCORE_GENERAL_MATCHED <= score < self.SCORE_KEYWORD_MATCHED]

        if keyword_links:
            self.logger.info(
                "HOMEPAGE-KEYWORDS: %s keyword links for %s (best: %s)",
                len(keyword_links), domain, scored_links[0][0] if scored_links else 0,
            )
            for url in keyword_links[:25]:
                norm = self._normalize_for_dedup(url)
                if norm not in self.visited:
                    self.visited.add(norm)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        priority=5,
                        meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
                    )

        elif leadership_links:
            self.logger.info(
                "HOMEPAGE-HINTS: No keyword links for %s, falling back to %s hint links",
                domain, len(leadership_links),
            )
            for url in leadership_links[:20]:
                norm = self._normalize_for_dedup(url)
                if norm not in self.visited:
                    self.visited.add(norm)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        priority=4,
                        meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
                    )

        else:
            self.logger.warning("No matches on homepage for %s, seeding hardcoded paths", domain)
            base = response.url.rstrip("/")
            for path in [
                "about", "team", "leadership", "about-us", "meet-our-team",
                "who-we-are", "management", "our-team", "executive-team",
                "company/team", "people", "staff", "directors", "board", "founders",
            ]:
                seed_url = f"{base}/{path}"
                norm = self._normalize_for_dedup(seed_url)
                if norm not in self.visited:
                    self.visited.add(norm)
                    yield scrapy.Request(
                        seed_url,
                        callback=self.parse_page,
                        errback=self.handle_request_error,
                        priority=3,
                        meta={"depth": 0, "root_url": root_url, "batch_index": batch_index},
                    )

    def _homepage_error(self, failure):
        root_url = failure.request.meta["root_url"]
        domain = failure.request.meta["domain"]
        self.logger.warning("Homepage request failed for %s: %s", domain, failure.value)

    # ─── Batch handling ──────────────────────────────────────
    def _start_batch(self, batch_index):
        start = batch_index * self.batch_size
        end = min(start + self.batch_size, len(self.all_urls))
        batch_urls = self.all_urls[start:end]
        if not batch_urls:
            return
        self.current_batch = batch_index
        self.logger.info("Starting batch %s with %s URLs", batch_index + 1, len(batch_urls))
        for url in batch_urls:
            domain = self.normalize_domain(url)
            parsed = urlparse(url)
            sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
            yield scrapy.Request(
                sitemap_url,
                callback=self.parse_sitemap,
                errback=self._sitemap_error,
                meta={"root_url": url, "domain": domain, "batch_index": batch_index},
                priority=10,
            )

    def start_requests(self):
        for url in self.all_urls:
            domain = self.normalize_domain(url)
            parsed = urlparse(url)
            sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
            yield scrapy.Request(
                sitemap_url,
                callback=self.parse_sitemap,
                errback=self._sitemap_error,
                meta={"root_url": url, "domain": domain, "batch_index": 0},
                priority=10,
            )

    def spider_idle(self, spider):
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

    # ─── Helper methods ──────────────────────────────────────
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
            raise FileNotFoundError(f"Keywords.json file not found in project root: {project_root}")
        with open(keyword_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        include_raw = data.get("include_keywords", [])
        self.keyword_category = {}
        if isinstance(include_raw, dict):
            include = {}
            for category, keywords in include_raw.items():
                category_keywords = [kw.lower() for kw in (keywords or [])]
                include[category] = category_keywords
                for kw in category_keywords:
                    self.keyword_category[kw] = category
        else:
            include = {"General": [kw.lower() for kw in include_raw]}
            self.keyword_category = {kw: "General" for kw in include["General"]}
        block = [kw.lower() for kw in data.get("block_keywords", [])]
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
                if col_name in ["company name", "agency name", "name", "business name"] and company_column is None:
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

                if domain not in self.domain_originals:
                    self.domain_originals[domain] = {}
                    for col in df.columns:
                        val = row.get(col)
                        if pd.notna(val):
                            self.domain_originals[domain][col] = str(val).strip()

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
            lambda match: chr(int(match.group(1), 16)), source_text,
        )
        decoded = cls.UNICODE_ESCAPE_REGEX.sub(
            lambda match: chr(int(match.group(1), 16)), decoded,
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
                normalized = unquote(email).strip().lower().replace("%40", "@")
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
            normalized_email = email.strip().lower().replace("mailto:", "").strip(" <>\"'(),;")
            if not normalized_email or normalized_email in seen:
                continue
            if self.is_trash_email(normalized_email):
                continue
            seen.add(normalized_email)
            candidates.append(normalized_email)
        return candidates

    def _extract_socials(self, response, domain):
        data = self.domain_data[domain]
        for link in response.css("a::attr(href)").getall():
            full_url = response.urljoin(link)
            parsed = urlparse(full_url)
            netloc = parsed.netloc.lower().replace("www.", "")
            if not netloc:
                continue
            matched = False
            for platform, domains in self.SOCIAL_DOMAINS.items():
                if any(d in netloc for d in domains):
                    data["socials"][platform].add(full_url)
                    matched = True
                    break
            if not matched:
                if any(d in netloc for d in self.OTHER_SOCIAL_DOMAINS):
                    data["socials"]["other"].add(full_url)

    def _extract_tel_phones(self, source_text, domain):
        data = self.domain_data[domain]
        for match in self.TEL_HREF_REGEX.findall(source_text):
            raw = match.strip()
            cleaned = re.sub(r"[^\d+]", "", raw)
            if cleaned:
                data["phones_scraped"].add(cleaned)

    def handle_request_error(self, failure):
        request = failure.request
        self.logger.warning("Request failed: %s (%s)", request.url, failure.value)

    # ─── Static scoring helpers ──────────────────────────────
    @staticmethod
    def leadership_score(url: str, text: str) -> int:
        score = 0
        data = (url + " " + text).lower()
        for term in ["our team", "leadership team", "executive team", "board of directors", "founder"]:
            if term in data:
                score += 10
        for hint in ["team", "people", "leadership", "board", "directors"]:
            if hint in url.lower():
                score += 5
        return score

    @staticmethod
    def contains_people(text: str) -> bool:
        if not text:
            return False
        signals = [
            "chief executive officer", "chief operating officer", "chief technology officer",
            "chief financial officer", "chief marketing officer", "managing director",
            "founder", "president", "director", "leadership team", "executive team",
            "board member", "chairman", "ceo", "cto", "cfo", "cmo", "coo",
        ]
        return sum(1 for s in signals if s in text.lower()) >= 2

    # ─── JSON-LD extraction ──────────────────────────────────
    def _extract_jsonld_people(self, response, domain):
        people = []
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Person":
                        name = item.get("name", "").strip()
                        title = item.get("jobTitle", "").strip()
                        if name and title:
                            people.append((name, title))
                    if item.get("@type") == "Organization":
                        for emp in item.get("employee", []):
                            if isinstance(emp, dict) and emp.get("@type") == "Person":
                                name = emp.get("name", "").strip()
                                title = emp.get("jobTitle", "").strip()
                                if name and title:
                                    people.append((name, title))
            except (json.JSONDecodeError, TypeError):
                continue

        if people:
            self.logger.info("JSON-LD: Found %s people on %s", len(people), response.url)
            names = [p[0] for p in people]
            titles = [p[1] for p in people]
            self.domain_ai_data[domain] = {
                "person_name": ", ".join(names),
                "job_title": ", ".join(titles),
                "service": "Not Found",
                "relevancy": "True",
                "sales_hook": "Not Found",
            }

    # ─── parse_page (with new keyword-match leadership logic) ──
    async def parse_page(self, response):
        root_url = response.meta["root_url"]
        domain = self.normalize_domain(root_url)
        batch_index = response.meta.get("batch_index", self.current_batch)
        depth = response.meta.get("depth", 0)

        outputs = []

        try:
            is_new_domain = domain not in self.domain_data
            if is_new_domain:
                self.domain_data[domain] = {
                    "include_count": 0,
                    "matched_keywords": set(),
                    "matched_categories": set(),
                    "blocked": False,
                    "emails": set(),
                    "leadership_text": "",
                    "general_text": "",
                    "leadership_seeded": False,
                    "socials": {
                        "twitter": set(), "linkedin": set(), "facebook": set(),
                        "instagram": set(), "youtube": set(), "other": set(),
                    },
                    "phones_scraped": set(),
                    "is_keyword_match": False,   # track if page has keyword match
                }

            # Check if this URL contains an include_keyword in its path
            keyword_match = self._url_has_keyword_match(response.url)
            self.domain_data[domain]["is_keyword_match"] = keyword_match

            self._extract_jsonld_people(response, domain)

            source_text = response.text
            text = source_text.lower()

            for keyword in self.block_keywords:
                if keyword in text:
                    self.domain_data[domain]["blocked"] = True
                    return outputs

            all_keywords = [
                keyword
                for keywords in self.include_keywords.values()
                for keyword in keywords
            ]

            count = sum(text.count(keyword) for keyword in all_keywords)
            self.domain_data[domain]["include_count"] += count

            for keyword in all_keywords:
                if keyword in text:
                    self.domain_data[domain]["matched_keywords"].add(keyword)
                    category = self.keyword_category.get(keyword)
                    if category:
                        self.domain_data[domain]["matched_categories"].add(category)

            page_score = self.leadership_score(response.url, source_text)
            has_people = self.contains_people(source_text)

            # ── NEW: Force leadership page if keyword match ──
            is_leadership_page = (page_score >= 15) or has_people or keyword_match

            if is_leadership_page:
                self.logger.info(
                    "LEADERSHIP PAGE | score=%s | people=%s | keyword_match=%s | %s",
                    page_score, has_people, keyword_match, response.url
                )

            # ── Collect leadership/general text ──
            if AI_ENABLED and len(self.domain_data[domain]["leadership_text"]) + len(self.domain_data[domain]["general_text"]) < self.AI_PAGE_TEXT_CAP:
                cleaned = _ai_clean_text(source_text)
                tagged = f"\n\n--- URL: {response.url} | TYPE: {'LEADERSHIP' if is_leadership_page else 'GENERAL'} | SCORE: {page_score} ---\n\n{cleaned}"
                if is_leadership_page:
                    current = self.domain_data[domain]["leadership_text"]
                    cap = self.AI_PAGE_TEXT_CAP // 2
                    self.domain_data[domain]["leadership_text"] = (current + tagged)[:cap]
                else:
                    current = self.domain_data[domain]["general_text"]
                    cap = self.AI_PAGE_TEXT_CAP // 2
                    self.domain_data[domain]["general_text"] = (current + tagged)[:cap]

            # ── Proactive seeding (fallback) ──
            if is_new_domain and not self.domain_data[domain].get("leadership_seeded"):
                self.domain_data[domain]["leadership_seeded"] = True
                base = response.url.rstrip("/")
                for path in [
                    "about", "team", "leadership", "about-us", "meet-our-team",
                    "who-we-are", "management", "our-team", "executive-team",
                    "company/team", "company/about", "people", "staff",
                    "directors", "board", "founders", "company/leadership",
                    "company/people", "who-we-are/leadership", "about/team"
                ]:
                    seed_url = f"{base}/{path}"
                    norm = self._normalize_for_dedup(seed_url)
                    if norm not in self.visited:
                        self.visited.add(norm)
                        outputs.append(scrapy.Request(
                            seed_url,
                            callback=self.parse_page,
                            errback=self.handle_request_error,
                            priority=300,
                            meta={
                                "depth": depth + 1,
                                "root_url": root_url,
                                "batch_index": batch_index,
                            },
                        ))

            self._extract_socials(response, domain)
            self._extract_tel_phones(source_text, domain)

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

            more_outputs = await self._after_dns_checks(results, response, domain, batch_index, depth, page_score, keyword_match)
            outputs.extend(more_outputs)

            if depth < 10:
                follow_requests = self._get_follow_requests(response, domain, batch_index, depth, page_score, keyword_match)
                outputs.extend(follow_requests)

        finally:
            self.logger.debug("Request finished: %s", response.url)

        return outputs

    def _validate_email_dns(self, email):
        email_domain = email.rsplit("@", 1)[-1]
        if self.email_domain_has_dns(email_domain):
            return email
        return None

    # ─── _after_dns_checks (with keyword-match early yield) ──
    async def _after_dns_checks(self, results, response, domain, batch_index, depth, page_score, keyword_match=False):
        for success, email_or_none in results:
            if success and email_or_none:
                self.domain_data[domain]["emails"].add(email_or_none)

        outputs = []
        data = self.domain_data[domain]

        if data["blocked"] or data["include_count"] < 2:
            if depth < 10:
                outputs.extend(self._get_follow_requests(response, domain, batch_index, depth, page_score, keyword_match))
            return outputs

        # ── Yield earlier if keyword match page ──
        has_leadership_text = len(data["leadership_text"]) > 200
        deep_enough = depth >= 2
        should_yield = has_leadership_text or deep_enough or keyword_match

        if domain not in self.yielded_domains and should_yield:
            self.yielded_domains.add(domain)
            item = await self._enrich_and_build_item(domain)
            outputs.append(item)

        if depth < 10:
            outputs.extend(self._get_follow_requests(response, domain, batch_index, depth, page_score, keyword_match))

        return outputs

    # ─── _get_follow_requests – aggressive on keyword-match ──
    def _get_follow_requests(self, response, domain, batch_index, depth, current_page_score=0, keyword_match=False):
        root_url = response.meta["root_url"]
        scored_requests = []
        seen = set()

        for link in response.css("a"):
            href = link.css("::attr(href)").get("")
            anchor_text = link.css("::text").get("") or ""
            anchor_text = re.sub(r"\s+", " ", anchor_text).strip()

            next_url = response.urljoin(href)
            norm = self._normalize_for_dedup(next_url)

            if norm in seen:
                continue
            seen.add(norm)

            if norm in self.visited:
                continue

            score, ok = self._score_link(next_url, anchor_text, domain, depth)
            if not ok:
                continue

            # ── If current page is keyword match, lower threshold to 8 ──
            if keyword_match:
                min_score = 8
            else:
                min_score = self.SCORE_KEYWORD_MATCHED if current_page_score < 15 else 8

            if score < min_score:
                continue

            self.visited.add(norm)

            priority = score + current_page_score

            if current_page_score >= 15:
                next_depth = depth
            else:
                next_depth = depth + 1

            scored_requests.append((
                priority,
                scrapy.Request(
                    next_url,
                    callback=self.parse_page,
                    errback=self.handle_request_error,
                    priority=priority,
                    meta={
                        "depth": next_depth,
                        "root_url": root_url,
                        "batch_index": batch_index,
                    },
                )
            ))

        scored_requests.sort(key=lambda x: x[0], reverse=True)
        return [req for _, req in scored_requests[:self.MAX_LINKS_PER_PAGE]]

    # ─── AI enrichment ────────────────────────────────────────
    async def _enrich_and_build_item(self, domain):
        if not AI_ENABLED:
            return self.build_item(domain, {})

        company_name = self.domain_companies.get(domain, "")
        leadership_text = self.domain_data[domain]["leadership_text"]
        general_text = self.domain_data[domain]["general_text"]

        self.logger.info("Running AI lead extraction for domain: %s", domain)
        ai_data = await maybe_deferred_to_future(
            threads.deferToThread(
                ai_get_lead_info, domain, company_name,
                leadership_text, general_text, self.target
            )
        )
        gc.collect()

        self.domain_ai_data[domain] = ai_data or {}
        return self.build_item(domain, ai_data)

    def build_item(self, domain, ai_data=None):
        data = self.domain_data[domain]
        original = self.domain_originals.get(domain, {})
        ai_data = ai_data or self.domain_ai_data.get(domain, {}) or {}

        def orig(field, default=""):
            val = original.get(field, "")
            return val if val and val.lower() not in ("nan",) else default

        def merge(field, ai_key=None, scraped=""):
            o = orig(field)
            if o and o not in ("Not Found", "False"):
                return o
            if ai_key and ai_data.get(ai_key) and ai_data[ai_key] not in ("Not Found", "False"):
                return ai_data[ai_key]
            if scraped:
                return scraped
            return "Not Found"

        orig_rel = orig("Relevancy")
        if orig_rel and orig_rel not in ("Not Found", "False"):
            relevancy = orig_rel
        else:
            relevancy = ai_data.get("relevancy", "False")
        socials = data["socials"]

        item = {
            "Website URL": domain,
            "Company Name": orig("Company Name", self.domain_companies.get(domain, "")),
            "Phone Number": orig("Phone Number", self.domain_phones.get(domain, "")),
            "Additional Phone Numbers": ", ".join(sorted(data["phones_scraped"])) if data["phones_scraped"] else "",
            "Person Name": merge("Person Name", "person_name"),
            "Job Title": merge("Job Title", "job_title"),
            "Relevancy": relevancy,
            "Service": merge("Service", "service"),
            "Sales Hook": ai_data.get("sales_hook", "Not Found"),
            "Keywords - Team": orig("Keywords - Team", ",".join(sorted(data["matched_keywords"]))),
            "Specification": orig("Specification", ",".join(sorted(data["matched_categories"]))),
            "Emails": orig("Emails", ",".join(sorted(data["emails"]))),
            "Twitter": ", ".join(sorted(socials["twitter"])),
            "LinkedIn": ", ".join(sorted(socials["linkedin"])),
            "Facebook": ", ".join(sorted(socials["facebook"])),
            "Instagram": ", ".join(sorted(socials["instagram"])),
            "YouTube": ", ".join(sorted(socials["youtube"])),
            "Other Social": ", ".join(sorted(socials["other"])),
        }

        self.logger.info(
            "YIELDING ITEM for domain: %s | Person: %s | Job: %s | Service: %s | Relevancy: %s | Hook: %s",
            domain,
            item["Person Name"], item["Job Title"], item["Service"],
            item["Relevancy"], item["Sales Hook"],
        )
        return item