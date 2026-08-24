# import os
# import re
# import time
# import html
# import json
# import base64
# import subprocess
# import sys
# from pathlib import Path
# from urllib.parse import urlparse, parse_qs
# from datetime import datetime, timezone

# import requests
# from bs4 import BeautifulSoup

# import g4f
# from g4f.client import Client

# # ============================================================
# # NIZAMI WEB RESEARCH - RELEVANCE-HARDENED v2
# # ============================================================
# # Important fixes over the previous version:
# #   1. Relevance is evaluated against the BEST matching search query,
# #      not only queries[0].
# #   2. Required entities are extracted conservatively and validated
# #      with normalized aliases.
# #   3. Required concepts are weighted but are not accidentally made
# #      impossible by long AI-generated phrases.
# #   4. Search results are deduplicated by canonical URL.
# #   5. Fresh web evidence is always preferred to OpenSearch cache.
# #   6. Cached documents are re-validated against the CURRENT question.
# #   7. Page-level relevance is checked after fetching.
# #   8. Exact phrase matching is separated from token matching.
# #   9. Search-engine rank is only a weak signal.
# #  10. Low-authority domains cannot win merely because they mention
# #      the right words.
# # ============================================================

# for stream in (sys.stdout, sys.stderr):
#     if hasattr(stream, "reconfigure"):
#         stream.reconfigure(encoding="utf-8", errors="replace")

# # ============================================================
# # CONFIGURATION
# # ============================================================

# MODEL = "gpt-4"
# WIDTH = 100
# ROOT = Path(__file__).resolve().parent

# OPENSEARCH_DIR = ROOT / "opensearch-2.17.1"
# OPENSEARCH_HOST = "127.0.0.1"
# OPENSEARCH_PORT = 9200
# OPENSEARCH_SCHEME = "https"
# OPENSEARCH_URL = f"{OPENSEARCH_SCHEME}://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}"

# OPENSEARCH_USERNAME = os.getenv("OPENSEARCH_USERNAME", "admin")
# OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "V3ctorForge!82Q")
# INDEX_NAME = "nizami_web_research"

# REQUEST_TIMEOUT = 20
# OPENSEARCH_TIMEOUT = 10

# MAX_SEARCH_RESULTS = 8
# MAX_FETCH_RESULTS = 6
# MAX_PAGE_CHARS = 14000
# MAX_EVIDENCE_CHARS = 8000
# MAX_LLM_CONTEXT = 14000
# MAX_PLANNED_SEARCH_QUERIES = 5
# MAX_TOTAL_SEARCH_CANDIDATES = 60

# MIN_RESULT_SCORE = 30.0
# MIN_QUERY_COVERAGE = 0.35

# USER_AGENT = (
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#     "AppleWebKit/537.36 (KHTML, like Gecko) "
#     "Chrome/151.0.0.0 Safari/537.36"
# )

# STOPWORDS = {
#     "a", "an", "and", "are", "as", "at", "be", "been", "being", "but",
#     "by", "can", "could", "did", "do", "does", "for", "from", "had", "has",
#     "have", "how", "i", "if", "in", "into", "is", "it", "its", "may", "me",
#     "more", "most", "my", "of", "on", "or", "our", "should", "that", "the",
#     "their", "them", "there", "these", "they", "this", "to", "was", "we",
#     "were", "what", "when", "where", "which", "who", "why", "will", "with",
#     "would", "you", "your", "please", "tell", "find", "give", "show",
#     "current", "currently", "today", "latest", "recent", "online", "web",
#     "search", "look", "up", "know", "information", "info"
# }

# GENERIC_CONCEPTS = {
#     "current", "currently", "today", "latest", "recent", "online", "web",
#     "information", "info", "about", "details", "person", "company"
# }

# ROLE_TERMS = {
#     "ceo": "CEO",
#     "chief executive officer": "CEO",
#     "founder": "founder",
#     "co-founder": "co-founder",
#     "owner": "owner",
#     "director": "director",
#     "president": "president",
#     "chairman": "chairman",
#     "chairperson": "chairperson",
#     "managing director": "managing director",
#     "executive": "executive",
#     "cto": "CTO",
#     "cfo": "CFO",
#     "coo": "COO",
#     "head": "head",
#     "leader": "leader",
#     "leadership": "leadership",
# }

# QUESTION_INTENT_TERMS = [
#     ("how much", "quantity"),
#     ("how many", "quantity"),
#     ("who is", "identity"),
#     ("who", "identity"),
#     ("what is", "fact"),
#     ("what", "fact"),
#     ("when", "date"),
#     ("where", "location"),
#     ("price", "price"),
#     ("cost", "price"),
#     ("latest", "current_events"),
#     ("recent", "current_events"),
#     ("news", "current_events"),
#     ("compare", "comparison"),
#     ("difference", "comparison"),
#     ("review", "review"),
# ]

# TRUSTED_DOMAIN_SCORES = {
#     "microsoft.com": 100,
#     "apple.com": 100,
#     "google.com": 100,
#     "amazon.com": 95,
#     "meta.com": 95,
#     "openai.com": 100,
#     "ibm.com": 95,
#     "intel.com": 95,
#     "nvidia.com": 95,
#     "oracle.com": 95,
#     "linkedin.com": 72,
#     "reuters.com": 72,
#     "apnews.com": 70,
#     "bbc.com": 68,
#     "bbc.co.uk": 68,
#     "bloomberg.com": 68,
#     "nytimes.com": 65,
#     "wsj.com": 65,
#     "forbes.com": 58,
#     "cnbc.com": 58,
#     "theguardian.com": 55,
#     "wikipedia.org": 35,
# }

# LOW_AUTHORITY_DOMAINS = {
#     "electricalvolt.com",
#     "current.com",
#     "example.com",
# }

# KNOWN_COMPANIES = {
#     "microsoft": "microsoft.com",
#     "apple": "apple.com",
#     "google": "google.com",
#     "amazon": "amazon.com",
#     "meta": "meta.com",
#     "openai": "openai.com",
#     "nvidia": "nvidia.com",
#     "ibm": "ibm.com",
#     "intel": "intel.com",
#     "oracle": "oracle.com",
#     "samsung": "samsung.com",
#     "tesla": "tesla.com",
#     "hyundai": "hyundai.com",
#     "toyota": "toyota.com",
# }

# OPENSEARCH_PROCESS = None
# AnyProvider = None
# LAST_RESEARCH = []
# LAST_QUERY = ""

# SESSION = requests.Session()
# SESSION.headers.update({"User-Agent": USER_AGENT})


# # ============================================================
# # UI
# # ============================================================

# def header(title):
#     print("\n" + "=" * WIDTH)
#     print(title.center(WIDTH))
#     print("=" * WIDTH)


# def section(title):
#     print("\n" + "-" * WIDTH)
#     print(title)
#     print("-" * WIDTH)


# def json_safe(value):
#     try:
#         return json.dumps(value, indent=2, ensure_ascii=False)
#     except Exception:
#         return str(value)


# # ============================================================
# # ANYPROVIDER
# # ============================================================

# def resolve_anyprovider():
#     try:
#         provider = getattr(g4f.Provider, "AnyProvider", None)
#         if provider:
#             return provider
#     except Exception:
#         pass

#     try:
#         from g4f.providers.any_provider import AnyProvider as ProviderClass
#         return ProviderClass
#     except Exception:
#         pass

#     try:
#         from g4f.Provider.any_provider import AnyProvider as ProviderClass
#         return ProviderClass
#     except Exception as e:
#         print(f"Could not locate AnyProvider: {type(e).__name__}: {e}")
#         raise SystemExit(1)


# # ============================================================
# # OPENSEARCH
# # ============================================================

# def opensearch_auth():
#     return OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD


# def opensearch_request(method, path, **kwargs):
#     url = OPENSEARCH_URL.rstrip("/") + "/" + path.lstrip("/")
#     kwargs.setdefault("timeout", OPENSEARCH_TIMEOUT)
#     kwargs.setdefault("verify", False)
#     kwargs.setdefault("auth", opensearch_auth())

#     try:
#         return SESSION.request(method, url, **kwargs)
#     except Exception:
#         return None


# def opensearch_ping():
#     r = opensearch_request("GET", "/")
#     return bool(r is not None and r.ok)


# def find_opensearch_bat():
#     candidates = [
#         OPENSEARCH_DIR / "bin" / "opensearch.bat",
#         OPENSEARCH_DIR / "distribution" / "archives" / "build" /
#         "install" / "opensearch" / "bin" / "opensearch.bat",
#     ]

#     for candidate in candidates:
#         if candidate.exists():
#             return candidate

#     try:
#         matches = list(OPENSEARCH_DIR.rglob("opensearch.bat"))
#         return matches[0] if matches else None
#     except Exception:
#         return None


# def show_opensearch_info():
#     r = opensearch_request("GET", "/")

#     if not r or not r.ok:
#         return False

#     try:
#         print("\nOpenSearch cluster information:")
#         print(json_safe(r.json()))
#     except Exception:
#         pass

#     return True


# def start_opensearch():
#     global OPENSEARCH_PROCESS

#     if opensearch_ping():
#         print("\nOpenSearch: ONLINE")
#         print(f"Endpoint: {OPENSEARCH_URL}")
#         return True

#     if not OPENSEARCH_DIR.exists():
#         print(f"OpenSearch directory does not exist: {OPENSEARCH_DIR}")
#         return False

#     start_script = find_opensearch_bat()

#     if not start_script:
#         print("opensearch.bat was not found.")
#         return False

#     log_dir = OPENSEARCH_DIR / "logs"
#     log_dir.mkdir(parents=True, exist_ok=True)

#     stdout_log = log_dir / "nizami_stdout.log"
#     stderr_log = log_dir / "nizami_stderr.log"

#     try:
#         stdout_file = open(
#             stdout_log, "a", encoding="utf-8", errors="replace"
#         )
#         stderr_file = open(
#             stderr_log, "a", encoding="utf-8", errors="replace"
#         )

#         launch_env = os.environ.copy()
#         launch_env["OPENSEARCH_INITIAL_ADMIN_PASSWORD"] = OPENSEARCH_PASSWORD

#         bundled_jdk = OPENSEARCH_DIR / "jdk"

#         if (bundled_jdk / "bin" / "java.exe").is_file():
#             launch_env["OPENSEARCH_JAVA_HOME"] = str(bundled_jdk)

#         OPENSEARCH_PROCESS = subprocess.Popen(
#             ["cmd.exe", "/c", str(start_script)],
#             cwd=str(start_script.parent.parent),
#             stdout=stdout_file,
#             stderr=stderr_file,
#             stdin=subprocess.DEVNULL,
#             env=launch_env,
#             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
#         )

#     except Exception as e:
#         print(f"FAILED to start OpenSearch: {type(e).__name__}: {e}")
#         return False

#     print(f"\nOpenSearch process PID: {OPENSEARCH_PROCESS.pid}")

#     for second in range(180):
#         if opensearch_ping():
#             print("\nOPENSEARCH ONLINE")
#             print(f"Endpoint : {OPENSEARCH_URL}")
#             print("TLS      : verify=False (local development)")
#             return True

#         if OPENSEARCH_PROCESS.poll() is not None:
#             print(
#                 f"OpenSearch process exited: "
#                 f"{OPENSEARCH_PROCESS.returncode}"
#             )
#             print(f"Logs: {stdout_log} / {stderr_log}")
#             return False

#         if second % 10 == 0:
#             print(f"  waiting... {second}s")

#         time.sleep(1)

#     print("OpenSearch did not become ready within 180 seconds.")
#     print(f"Logs: {stdout_log} / {stderr_log}")

#     return False


# def create_index():
#     if not opensearch_ping():
#         return False

#     mapping = {
#         "settings": {
#             "index": {
#                 "number_of_shards": 1,
#                 "number_of_replicas": 0
#             }
#         },
#         "mappings": {
#             "properties": {
#                 "title": {"type": "text"},
#                 "url": {"type": "keyword"},
#                 "domain": {"type": "keyword"},
#                 "snippet": {"type": "text"},
#                 "content": {"type": "text"},
#                 "evidence": {"type": "text"},
#                 "query": {"type": "text"},
#                 "search_query": {"type": "text"},
#                 "engine": {"type": "keyword"},
#                 "engine_rank": {"type": "integer"},
#                 "intent": {"type": "keyword"},
#                 "required_entities": {"type": "keyword"},
#                 "required_concepts": {"type": "keyword"},
#                 "timestamp": {"type": "date"},
#             }
#         },
#     }

#     head = opensearch_request("HEAD", f"/{INDEX_NAME}")

#     if head is not None and head.status_code == 200:
#         return True

#     r = opensearch_request(
#         "PUT",
#         f"/{INDEX_NAME}",
#         json=mapping
#     )

#     if not r or r.status_code not in (200, 201):
#         print("Index creation failed:")
#         print(r.text if r else "No response")
#         return False

#     return True


# # ============================================================
# # TEXT / URL HELPERS
# # ============================================================

# def clean_text(text):
#     if not text:
#         return ""

#     text = html.unescape(str(text))
#     text = re.sub(r"\s+", " ", text)

#     return text.strip()


# def tokenize(text):
#     return re.findall(
#         r"[a-z0-9]+(?:[._'-][a-z0-9]+)*",
#         text.lower()
#     )


# def meaningful_tokens(text):
#     return {
#         t
#         for t in tokenize(text)
#         if len(t) >= 3 and t not in STOPWORDS
#     }


# def exact_word_present(term, text):
#     term = clean_text(term).lower()
#     text = clean_text(text).lower()

#     if not term:
#         return False

#     return bool(
#         re.search(
#             r"(?<!\w)" + re.escape(term) + r"(?!\w)",
#             text
#         )
#     )


# def phrase_tokens(term):
#     return [
#         t for t in tokenize(term)
#         if len(t) >= 2 and t not in STOPWORDS
#     ]


# def normalize_url(url):
#     if not url:
#         return ""

#     url = html.unescape(url).strip()

#     if "bing.com/ck/a" in url:
#         try:
#             parsed = urlparse(url)
#             params = parse_qs(parsed.query)
#             encoded = params.get("u", [])

#             if encoded:
#                 value = encoded[0]

#                 if value.startswith("a1"):
#                     decoded = base64.b64decode(
#                         value[2:] + "=="
#                     ).decode(
#                         "utf-8",
#                         errors="ignore"
#                     )

#                     if decoded.startswith(("http://", "https://")):
#                         url = decoded

#         except Exception:
#             pass

#     try:
#         parsed = urlparse(url)._replace(fragment="")
#         return parsed.geturl()
#     except Exception:
#         return url


# def canonical_url(url):
#     url = normalize_url(url)

#     if not url:
#         return ""

#     try:
#         p = urlparse(url)

#         scheme = p.scheme.lower()
#         netloc = p.netloc.lower()

#         if netloc.startswith("www."):
#             netloc = netloc[4:]

#         path = p.path.rstrip("/") or "/"

#         ignored_params = {
#             "utm_source",
#             "utm_medium",
#             "utm_campaign",
#             "utm_term",
#             "utm_content",
#             "gclid",
#             "fbclid",
#         }

#         query_parts = []

#         for key, values in parse_qs(
#             p.query,
#             keep_blank_values=True
#         ).items():

#             if key.lower() in ignored_params:
#                 continue

#             for value in values:
#                 query_parts.append(
#                     f"{key}={value}"
#                 )

#         query = "&".join(sorted(query_parts))

#         return (
#             f"{scheme}://{netloc}{path}"
#             + (f"?{query}" if query else "")
#         )

#     except Exception:
#         return url


# def get_domain(url):
#     try:
#         return urlparse(url).netloc.lower().removeprefix("www.")
#     except Exception:
#         return ""


# # ============================================================
# # QUERY NORMALIZATION + ANALYSIS
# # ============================================================

# def normalize_query(user_input):
#     query = clean_text(user_input)

#     prefixes = [
#         r"^/search\s+",
#         r"^search\s+the\s+web\s+for\s+",
#         r"^search\s+the\s+web\s+",
#         r"^web\s+search\s+for\s+",
#         r"^web\s+search\s+",
#         r"^search\s+online\s+for\s+",
#         r"^search\s+online\s+",
#         r"^google\s+this\s+",
#         r"^look\s+this\s+up\s+",
#         r"^look\s+it\s+up\s+",
#         r"^do\s+a\s+web\s+search\s+and\s+find\s+",
#         r"^do\s+a\s+web\s+search\s+",
#         r"^find\s+",
#     ]

#     for pattern in prefixes:
#         new_query = re.sub(
#             pattern,
#             "",
#             query,
#             flags=re.IGNORECASE
#         )

#         if new_query != query:
#             query = new_query
#             break

#     return clean_text(query.strip(" ?!.")).strip()


# def detect_intent(question):
#     lower = question.lower()

#     for term, intent in QUESTION_INTENT_TERMS:
#         if re.search(
#             r"(?<!\w)" + re.escape(term) + r"(?!\w)",
#             lower
#         ):
#             return intent

#     return "general_fact"


# def extract_capitalized_entities(question):
#     entities = []

#     patterns = [
#         r"\b[A-Z][A-Za-z0-9&.-]*(?:\s+[A-Z][A-Za-z0-9&.-]*){0,4}",
#     ]

#     for pattern in patterns:
#         for match in re.findall(pattern, question):
#             candidate = clean_text(match).strip(" ,.!?:")

#             if not candidate:
#                 continue

#             if candidate.lower() in {
#                 "who", "what", "when", "where", "why", "how",
#                 "tell", "find", "search", "please"
#             }:
#                 continue

#             # Roles are concepts, not named entities.
#             if len(tokenize(candidate)) == 1 and any(
#                 candidate.lower() == r.lower()
#                 for r in ROLE_TERMS
#             ):
#                 continue

#             if len(candidate) >= 3:
#                 entities.append(candidate)

#     return dedupe_strings(entities)


# def local_query_analysis(question):
#     q = normalize_query(question)

#     entities = []
#     concepts = []

#     # Known organizations are strong entities.
#     for name in KNOWN_COMPANIES:
#         if exact_word_present(name, q):
#             entities.append(name)

#     # Add likely proper-name entities from the user's original wording.
#     for entity in extract_capitalized_entities(question):
#         if entity.lower() not in {
#             x.lower() for x in entities
#         }:
#             entities.append(entity)

#     # Role concepts are normalized to canonical forms.
#     for role in sorted(
#         ROLE_TERMS,
#         key=len,
#         reverse=True
#     ):
#         if exact_word_present(role, q):
#             concepts.append(ROLE_TERMS[role])

#     # Roles must never become hard named-entity requirements.
#     role_names = {r.lower() for r in ROLE_TERMS}
#     entities = [e for e in entities if e.lower() not in role_names]

#     # If there is a company entity and a role, the role is enough.
#     # Do not add generic question words as concepts.
#     concepts = [
#         c for c in dedupe_strings(concepts)
#         if c.lower() not in GENERIC_CONCEPTS
#     ]

#     return {
#         "intent": detect_intent(q),
#         "entities": entities[:8],
#         "concepts": concepts[:8],
#     }


# def extract_response_content(response):
#     try:
#         choices = getattr(response, "choices", None)

#         if not choices:
#             return ""

#         message = getattr(
#             choices[0],
#             "message",
#             None
#         )

#         content = getattr(
#             message,
#             "content",
#             None
#         )

#         return content.strip() if content else ""

#     except Exception:
#         return ""


# # ============================================================
# # DEDUPLICATION
# # ============================================================

# def dedupe_strings(items):
#     out = []
#     seen = set()

#     for item in items:
#         item = clean_text(item)

#         if not item:
#             continue

#         key = item.lower()

#         if key not in seen:
#             seen.add(key)
#             out.append(item)

#     return out


# # ============================================================
# # SEARCH QUERY VALIDATION
# # ============================================================

# def query_contains_entity(query, entity):
#     if exact_word_present(entity, query):
#         return True

#     # Known company aliases.
#     aliases = {
#         "microsoft": {"microsoft", "msft"},
#         "apple": {"apple", "apple inc"},
#         "google": {"google", "alphabet"},
#         "meta": {"meta", "facebook"},
#         "openai": {"openai"},
#         "amazon": {"amazon", "amazon.com"},
#         "nvidia": {"nvidia"},
#         "ibm": {"ibm"},
#         "intel": {"intel"},
#         "oracle": {"oracle"},
#     }

#     for alias in aliases.get(
#         entity.lower(),
#         {entity.lower()}
#     ):
#         if exact_word_present(alias, query):
#             return True

#     return False


# def query_contains_concept(query, concept):
#     if exact_word_present(concept, query):
#         return True

#     # Allow token overlap for long concepts.
#     tokens = phrase_tokens(concept)

#     if len(tokens) >= 2:
#         q_tokens = set(tokenize(query))
#         matched = sum(
#             1 for token in tokens
#             if token in q_tokens
#         )

#         return matched >= max(1, len(tokens) // 2)

#     return False


# def validate_search_query(
#     candidate,
#     required_entities,
#     required_concepts
# ):
#     candidate = clean_text(candidate)

#     if len(candidate) < 3:
#         return False

#     for entity in required_entities:
#         if not query_contains_entity(
#             candidate,
#             entity
#         ):
#             return False

#     if required_concepts:
#         if not any(
#             query_contains_concept(
#                 candidate,
#                 concept
#             )
#             for concept in required_concepts
#         ):
#             return False

#     return True


# # ============================================================
# # DETERMINISTIC QUERY GENERATION
# # ============================================================

# def get_domain_for_entity(entity):
#     return KNOWN_COMPANIES.get(
#         entity.lower(),
#         ""
#     )


# def generate_deterministic_queries(
#     question,
#     entities,
#     concepts,
#     intent
# ):
#     q = clean_text(question)
#     queries = []

#     if entities:
#         entity = entities[0]

#         if concepts:
#             concept = concepts[0]

#             queries.append(
#                 f'"{entity}" "{concept}"'
#             )
#             queries.append(
#                 f"{entity} {concept}"
#             )

#             domain = get_domain_for_entity(entity)

#             if domain:
#                 queries.append(
#                     f"site:{domain} {entity} {concept}"
#                 )

#         else:
#             queries.append(
#                 f'"{entity}"'
#             )

#     if not queries:
#         queries.append(q)

#     # Add a date only for time-sensitive requests.
#     if intent in {
#         "current_events",
#         "date"
#     }:
#         queries.append(
#             f"{q} 2026"
#         )

#     validated = []

#     for candidate in queries:
#         if validate_search_query(
#             candidate,
#             entities,
#             concepts
#         ):
#             validated.append(candidate)

#     # The original query is useful if it passes.
#     if validate_search_query(
#         q,
#         entities,
#         concepts
#     ):
#         validated.append(q)

#     return dedupe_strings(
#         validated
#     )[:MAX_PLANNED_SEARCH_QUERIES]


# # ============================================================
# # AI QUERY PLANNING
# # ============================================================

# def plan_search_queries(client, question):
#     q = normalize_query(question)
#     local = local_query_analysis(q)

#     print(
#         "Local analysis: "
#         f"intent={local['intent']} | "
#         f"entities={local['entities']} | "
#         f"concepts={local['concepts']}"
#     )

#     prompt = f"""
# You are ONLY a web-search query planner.
# You are NOT answering the question.

# Return ONLY valid JSON:

# {{
#   "intent": "short intent",
#   "entities": ["specific named entities"],
#   "concepts": ["specific requested attributes"],
#   "queries": ["2 to {MAX_PLANNED_SEARCH_QUERIES} search queries"]
# }}

# Rules:
# 1. Preserve every important named entity.
# 2. Preserve the requested attribute.
# 3. Never replace a company/person/product with a generic term.
# 4. Never invent a different subject.
# 5. Queries must be independently useful to Bing or DuckDuckGo.
# 6. For executive questions, every query must contain the company
#    and requested role.
# 7. Roles such as CEO, founder, director, and leadership are concepts,
#    NEVER named entities.
# 8. Use official-site queries when useful.
# 8. Do not use generic words such as "current", "about", "person",
#    "information" as the only relevance signal.
# 9. Do not answer the question.

# USER QUESTION:
# {q}

# LOCAL ANALYSIS:
# {json.dumps(local, ensure_ascii=False)}
# """.strip()

#     try:
#         response = client.chat.completions.create(
#             model=MODEL,
#             provider=AnyProvider,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": (
#                         "Return valid JSON only. "
#                         "Plan retrieval; do not answer."
#                     ),
#                 },
#                 {
#                     "role": "user",
#                     "content": prompt,
#                 },
#             ],
#             stream=False,
#         )

#         raw = extract_response_content(response)

#         raw = re.sub(
#             r"^```(?:json)?\s*|\s*```$",
#             "",
#             raw.strip(),
#             flags=re.IGNORECASE
#         )

#         data = json.loads(raw)

#         if not isinstance(data, dict):
#             raise ValueError(
#                 "Planner did not return an object."
#             )

#         intent = (
#             clean_text(
#                 data.get("intent", "")
#             )
#             or local["intent"]
#         )

#         ai_entities = [
#             clean_text(x)
#             for x in data.get("entities", [])
#             if isinstance(x, str)
#         ]

#         ai_concepts = [
#             clean_text(x)
#             for x in data.get("concepts", [])
#             if isinstance(x, str)
#         ]

#         ai_queries = [
#             clean_text(x)[:300]
#             for x in data.get("queries", [])
#             if isinstance(x, str)
#         ]

#         role_names = {r.lower() for r in ROLE_TERMS}

#         safe_ai_entities = [
#             x for x in ai_entities
#             if x.lower() not in role_names
#             and x.lower() not in GENERIC_CONCEPTS
#         ]

#         entities = dedupe_strings(
#             local["entities"] + safe_ai_entities
#         )[:10]

#         # Only deterministic/local concepts are hard gates.
#         # AI-generated descriptions such as "CEO identity" are not
#         # reliable enough to reject otherwise relevant pages.
#         concepts = dedupe_strings(local["concepts"])[:8]

#         valid = []

#         for candidate in ai_queries:
#             if validate_search_query(
#                 candidate,
#                 entities,
#                 concepts
#             ):
#                 valid.append(candidate)

#         valid.extend(
#             generate_deterministic_queries(
#                 q,
#                 entities,
#                 concepts,
#                 intent
#             )
#         )

#         valid = dedupe_strings(valid)[
#             :MAX_PLANNED_SEARCH_QUERIES
#         ]

#         if valid:
#             print("AI retrieval plan (validated):")

#             for i, item in enumerate(
#                 valid,
#                 1
#             ):
#                 print(
#                     f"  {i}. {item}"
#                 )

#             print(
#                 f"Required entities: {entities}"
#             )
#             print(
#                 f"Required concepts: {concepts}"
#             )

#             return {
#                 "intent": intent,
#                 "entities": entities,
#                 "concepts": concepts,
#                 "queries": valid,
#             }

#     except Exception as e:
#         print(
#             "AI query planner unavailable; "
#             f"using deterministic fallback "
#             f"({type(e).__name__})."
#         )

#     local_queries = generate_deterministic_queries(
#         q,
#         local["entities"],
#         local["concepts"],
#         local["intent"]
#     )

#     return {
#         "intent": local["intent"],
#         "entities": local["entities"],
#         "concepts": local["concepts"],
#         "queries": local_queries,
#     }


# # ============================================================
# # SEARCH DETECTION
# # ============================================================

# def should_search(text):
#     lower = text.lower()

#     explicit = [
#         "/search",
#         "search the web",
#         "web search",
#         "search online",
#         "look this up",
#         "look it up",
#         "find online",
#         "browse the web",
#         "latest",
#         "currently",
#         "today",
#         "recent",
#         "news",
#         "price",
#         "linkedin",
#         "who is the ceo",
#         "who is ceo",
#         "ceo of",
#         "founder of",
#         "owner of",
#         "director of",
#         "leadership of",
#         "leadership team",
#         "leadership",
#         "executive team",
#         "management of",
#         "management team",
#         "who leads",
#     ]

#     return any(
#         trigger in lower
#         for trigger in explicit
#     )


# # ============================================================
# # BING
# # ============================================================

# def bing_search(
#     query,
#     max_results=8
# ):
#     try:
#         r = SESSION.get(
#             "https://www.bing.com/search",
#             params={
#                 "q": query,
#                 "count": max_results
#             },
#             headers={
#                 "Accept": (
#                     "text/html,application/xhtml+xml,"
#                     "application/xml;q=0.9,*/*;q=0.8"
#                 ),
#                 "Accept-Language":
#                     "en-US,en;q=0.9",
#             },
#             timeout=REQUEST_TIMEOUT,
#         )

#         r.raise_for_status()

#     except Exception:
#         return []

#     soup = BeautifulSoup(
#         r.text,
#         "html.parser"
#     )

#     results = []

#     for item in soup.select(
#         "li.b_algo"
#     ):
#         link = item.select_one(
#             "h2 a"
#         )

#         if not link:
#             continue

#         href = normalize_url(
#             link.get("href", "")
#         )

#         title = clean_text(
#             link.get_text(
#                 " ",
#                 strip=True
#             )
#         )

#         snippet_el = item.select_one(
#             ".b_caption p"
#         )

#         snippet = (
#             clean_text(
#                 snippet_el.get_text(
#                     " ",
#                     strip=True
#                 )
#             )
#             if snippet_el
#             else ""
#         )

#         if not href:
#             continue

#         results.append({
#             "title": title,
#             "url": href,
#             "snippet": snippet,
#             "engine": "Bing",
#             "engine_rank": len(results) + 1,
#             "search_query": query,
#         })

#         if len(results) >= max_results:
#             break

#     return results


# # ============================================================
# # DUCKDUCKGO
# # ============================================================

# def duckduckgo_search(
#     query,
#     max_results=8
# ):
#     try:
#         r = SESSION.get(
#             "https://html.duckduckgo.com/html/",
#             params={"q": query},
#             headers={
#                 "Accept": (
#                     "text/html,application/xhtml+xml,"
#                     "application/xml;q=0.9,*/*;q=0.8"
#                 ),
#                 "Accept-Language":
#                     "en-US,en;q=0.9",
#             },
#             timeout=REQUEST_TIMEOUT,
#         )

#         r.raise_for_status()

#     except Exception:
#         return []

#     soup = BeautifulSoup(
#         r.text,
#         "html.parser"
#     )

#     results = []

#     for item in soup.select(
#         ".result"
#     ):
#         link = item.select_one(
#             ".result__a"
#         )

#         if not link:
#             continue

#         href = normalize_url(
#             link.get("href", "")
#         )

#         title = clean_text(
#             link.get_text(
#                 " ",
#                 strip=True
#             )
#         )

#         snippet_el = item.select_one(
#             ".result__snippet"
#         )

#         snippet = (
#             clean_text(
#                 snippet_el.get_text(
#                     " ",
#                     strip=True
#                 )
#             )
#             if snippet_el
#             else ""
#         )

#         if not href:
#             continue

#         results.append({
#             "title": title,
#             "url": href,
#             "snippet": snippet,
#             "engine": "DuckDuckGo",
#             "engine_rank": len(results) + 1,
#             "search_query": query,
#         })

#         if len(results) >= max_results:
#             break

#     return results


# # ============================================================
# # RELEVANCE ENGINE
# # ============================================================

# def text_for_result(result):
#     return " ".join([
#         result.get("title", ""),
#         result.get("snippet", ""),
#         result.get("content", ""),
#         result.get("evidence", ""),
#         result.get("url", ""),
#     ])


# def query_coverage(query, result):
#     q_tokens = meaningful_tokens(query)

#     if not q_tokens:
#         return 0.0

#     title_tokens = set(
#         tokenize(result.get("title", ""))
#     )
#     snippet_tokens = set(
#         tokenize(result.get("snippet", ""))
#     )
#     content_tokens = set(
#         tokenize(
#             result.get("content", "")
#         )
#     )
#     url_tokens = set(
#         tokenize(result.get("url", ""))
#     )

#     matched = 0

#     for token in q_tokens:
#         if (
#             token in title_tokens
#             or token in snippet_tokens
#             or token in content_tokens
#             or token in url_tokens
#         ):
#             matched += 1

#     return matched / len(q_tokens)


# def entity_coverage(result, entities):
#     if not entities:
#         return 1.0

#     text = text_for_result(result)

#     matched = sum(
#         1
#         for entity in entities
#         if exact_word_present(
#             entity,
#             text
#         )
#         or query_contains_entity(
#             text,
#             entity
#         )
#     )

#     return matched / len(entities)


# def concept_matches_text(concept, text):
#     if exact_word_present(concept, text):
#         return True

#     aliases = {
#         "ceo": {"ceo", "chief executive officer", "chief executive"},
#         "founder": {"founder", "co-founder", "cofounder", "founded by"},
#         "director": {"director", "managing director"},
#         "president": {"president"},
#         "chairman": {"chairman", "chairperson"},
#         "leadership": {"leadership", "management", "executive team", "leadership team"},
#         "executive": {"executive", "executive leadership"},
#     }

#     return any(
#         exact_word_present(alias, text)
#         for alias in aliases.get(concept.lower(), {concept.lower()})
#     )


# def concept_coverage(result, concepts):
#     if not concepts:
#         return 1.0

#     text = text_for_result(result)
#     matched = sum(
#         1 for concept in concepts
#         if concept_matches_text(concept, text)
#     )
#     return matched / len(concepts)


# def domain_authority(domain):
#     if domain in TRUSTED_DOMAIN_SCORES:
#         return TRUSTED_DOMAIN_SCORES[domain]

#     for root in TRUSTED_DOMAIN_SCORES:
#         if domain.endswith("." + root):
#             return 45

#     if domain in LOW_AUTHORITY_DOMAINS:
#         return -80

#     return 0


# def best_query_coverage(
#     result,
#     queries
# ):
#     if not queries:
#         return 0.0, ""

#     scored = [
#         (
#             query_coverage(
#                 q,
#                 result
#             ),
#             q
#         )
#         for q in queries
#     ]

#     return max(
#         scored,
#         key=lambda x: x[0]
#     )


# def hard_relevance_gate(
#     result,
#     analysis
# ):
#     entities = analysis.get(
#         "entities",
#         []
#     )

#     concepts = analysis.get(
#         "concepts",
#         []
#     )

#     queries = analysis.get(
#         "queries",
#         []
#     )

#     ecover = entity_coverage(
#         result,
#         entities
#     )

#     ccover = concept_coverage(
#         result,
#         concepts
#     )

#     coverage, _ = best_query_coverage(
#         result,
#         queries
#     )

#     if entities and ecover < 1.0:
#         return False, "missing required entity"

#     if concepts and ccover < 1.0:
#         return False, "missing required concept"

#     # Entity/concept matches can compensate for lower query coverage.
#     if coverage < MIN_QUERY_COVERAGE:
#         if not (
#             entities
#             and concepts
#             and ecover == 1.0
#             and ccover == 1.0
#         ):
#             return False, "low query coverage"

#     return True, "ok"


# def rank_results(
#     results,
#     question,
#     analysis
# ):
#     ranked = []

#     entities = analysis.get(
#         "entities",
#         []
#     )

#     concepts = analysis.get(
#         "concepts",
#         []
#     )

#     queries = analysis.get(
#         "queries",
#         []
#     ) or [question]

#     for result in results:
#         title = result.get(
#             "title",
#             ""
#         )

#         snippet = result.get(
#             "snippet",
#             ""
#         )

#         url = result.get(
#             "url",
#             ""
#         )

#         domain = get_domain(url)

#         allowed, reason = hard_relevance_gate(
#             result,
#             analysis
#         )

#         if not allowed:
#             result["rejected_reason"] = reason
#             continue

#         ecover = entity_coverage(
#             result,
#             entities
#         )

#         ccover = concept_coverage(
#             result,
#             concepts
#         )

#         best_coverage, best_query = (
#             best_query_coverage(
#                 result,
#                 queries
#             )
#         )

#         title_lower = title.lower()
#         snippet_lower = snippet.lower()

#         exact_phrase = 0

#         for q in queries:
#             phrase = clean_text(
#                 q
#             ).lower()

#             if not phrase:
#                 continue

#             if phrase in title_lower:
#                 exact_phrase = max(
#                     exact_phrase,
#                     2
#                 )
#             elif phrase in snippet_lower:
#                 exact_phrase = max(
#                     exact_phrase,
#                     1
#                 )

#         score = 0.0

#         # Core relevance.
#         score += best_coverage * 50
#         score += ecover * 40
#         score += ccover * 30

#         # Exact phrase is useful but cannot override relevance.
#         score += exact_phrase * 12

#         # Domain authority is deliberately capped.
#         authority = domain_authority(
#             domain
#         )

#         score += min(
#             authority,
#             100
#         ) * 0.25

#         # Search-engine position is weak only.
#         try:
#             engine_rank = int(
#                 result.get(
#                     "engine_rank",
#                     10
#                 )
#                 or 10
#             )
#         except Exception:
#             engine_rank = 10

#         score += max(
#             0.0,
#             10.0 - engine_rank
#         )

#         # Entity in title is strong.
#         for entity in entities:
#             if query_contains_entity(
#                 title,
#                 entity
#             ):
#                 score += 22

#         # Concept in title is useful.
#         for concept in concepts:
#             if exact_word_present(
#                 concept,
#                 title
#             ):
#                 score += 18

#         lower_url = url.lower()

#         if any(
#             part in lower_url
#             for part in (
#                 "/leadership",
#                 "/about",
#                 "/company",
#                 "/management",
#                 "/executive",
#                 "/people",
#                 "/team",
#             )
#         ):
#             score += 8

#         if domain in LOW_AUTHORITY_DOMAINS:
#             score -= 80

#         result["best_query"] = best_query
#         result["query_coverage"] = round(
#             best_coverage,
#             3
#         )
#         result["entity_coverage"] = round(
#             ecover,
#             3
#         )
#         result["concept_coverage"] = round(
#             ccover,
#             3
#         )
#         result["score"] = round(
#             score,
#             2
#         )

#         # Do not allow very weak results into final retrieval.
#         if score < MIN_RESULT_SCORE:
#             result["rejected_reason"] = (
#                 f"score below threshold ({score:.2f})"
#             )
#             continue

#         ranked.append(result)

#     ranked.sort(
#         key=lambda x: (
#             -float(
#                 x.get(
#                     "score",
#                     0
#                 )
#             ),
#             x.get(
#                 "engine_rank",
#                 999
#             ),
#         )
#     )

#     return ranked[:MAX_SEARCH_RESULTS]


# # ============================================================
# # WEB SEARCH PIPELINE
# # ============================================================

# def web_search(
#     question,
#     analysis
# ):
#     start = time.perf_counter()

#     all_results = []
#     seen_urls = set()

#     queries = analysis.get(
#         "queries",
#         []
#     )

#     print("\nWEB RESEARCH")
#     print(
#         f"Entity query: {question}"
#     )

#     for qi, search_query in enumerate(
#         queries,
#         1
#     ):
#         print(
#             f'\nSearch {qi}/{len(queries)}: '
#             f'"{search_query}"'
#         )

#         engines = [
#             bing_search,
#             duckduckgo_search,
#         ]

#         for engine in engines:
#             for result in engine(
#                 search_query
#             ):
#                 url = normalize_url(
#                     result.get(
#                         "url",
#                         ""
#                     )
#                 )

#                 key = canonical_url(url)

#                 if not key:
#                     continue

#                 domain = get_domain(
#                     url
#                 )

#                 if domain in {
#                     "bing.com",
#                     "duckduckgo.com",
#                 }:
#                     continue

#                 if key in seen_urls:
#                     continue

#                 seen_urls.add(key)

#                 result["original_query"] = (
#                     question
#                 )

#                 all_results.append(
#                     result
#                 )

#                 if len(all_results) >= (
#                     MAX_TOTAL_SEARCH_CANDIDATES
#                 ):
#                     break

#             if len(all_results) >= (
#                 MAX_TOTAL_SEARCH_CANDIDATES
#             ):
#                 break

#         if len(all_results) >= (
#             MAX_TOTAL_SEARCH_CANDIDATES
#         ):
#             break

#     ranked = rank_results(
#         all_results,
#         question,
#         analysis
#     )

#     elapsed = (
#         time.perf_counter()
#         - start
#     )

#     print(
#         f"\nSEARCH COMPLETE | "
#         f"{len(ranked)} validated results | "
#         f"{elapsed:.2f}s"
#     )

#     return ranked


# # ============================================================
# # WEB FETCHING
# # ============================================================

# def fetch_page(url):
#     start = time.perf_counter()

#     try:
#         r = SESSION.get(
#             url,
#             headers={
#                 "Accept": (
#                     "text/html,application/xhtml+xml,"
#                     "application/xml;q=0.9,*/*;q=0.8"
#                 ),
#                 "Accept-Language":
#                     "en-US,en;q=0.9",
#             },
#             timeout=REQUEST_TIMEOUT,
#             allow_redirects=True,
#         )

#         r.raise_for_status()

#     except Exception as e:
#         return {
#             "success": False,
#             "url": url,
#             "error": (
#                 f"{type(e).__name__}: {e}"
#             ),
#             "content": "",
#             "elapsed": (
#                 time.perf_counter()
#                 - start
#             ),
#         }

#     content_type = (
#         r.headers
#         .get(
#             "Content-Type",
#             ""
#         )
#         .lower()
#     )

#     if (
#         "text/html" not in content_type
#         and "application/xhtml" not in content_type
#     ):
#         return {
#             "success": False,
#             "url": r.url,
#             "error": (
#                 "Unsupported content type: "
#                 f"{content_type}"
#             ),
#             "content": "",
#             "elapsed": (
#                 time.perf_counter()
#                 - start
#             ),
#         }

#     soup = BeautifulSoup(
#         r.text,
#         "html.parser"
#     )

#     for tag in soup(
#         [
#             "script",
#             "style",
#             "noscript",
#             "svg",
#             "nav",
#             "footer",
#             "header",
#             "aside",
#             "form",
#         ]
#     ):
#         tag.decompose()

#     title = (
#         clean_text(
#             soup.title.get_text(
#                 " ",
#                 strip=True
#             )
#         )
#         if soup.title
#         else ""
#     )

#     metadata = []

#     for meta in soup.find_all(
#         "meta"
#     ):
#         key = (
#             meta.get("name", "")
#             or meta.get(
#                 "property",
#                 ""
#             )
#         ).lower()

#         content = meta.get(
#             "content",
#             ""
#         )

#         if (
#             key in {
#                 "description",
#                 "og:description",
#                 "author",
#                 "og:title",
#             }
#             and content
#         ):
#             metadata.append(
#                 clean_text(content)
#             )

#     body = clean_text(
#         soup.get_text(
#             " ",
#             strip=True
#         )
#     )

#     content = clean_text(
#         " ".join(
#             metadata
#         )
#         + " "
#         + body
#     )[:MAX_PAGE_CHARS]

#     return {
#         "success": True,
#         "url": r.url,
#         "title": title,
#         "content": content,
#         "elapsed": (
#             time.perf_counter()
#             - start
#         ),
#         "status": r.status_code,
#     }


# # ============================================================
# # EVIDENCE EXTRACTION
# # ============================================================

# def extract_evidence(
#     page,
#     question,
#     analysis
# ):
#     content = page.get(
#         "content",
#         ""
#     )

#     if not content:
#         return ""

#     sentences = re.split(
#         r"(?<=[.!?])\s+",
#         content
#     )

#     entities = analysis.get(
#         "entities",
#         []
#     )

#     concepts = analysis.get(
#         "concepts",
#         []
#     )

#     q_tokens = meaningful_tokens(
#         question
#     )

#     scored = []

#     for sentence in sentences:
#         sentence = clean_text(
#             sentence
#         )

#         if len(sentence) < 35:
#             continue

#         score = 0.0

#         for token in q_tokens:
#             if exact_word_present(
#                 token,
#                 sentence
#             ):
#                 score += 1.5

#         for entity in entities:
#             if query_contains_entity(
#                 sentence,
#                 entity
#             ):
#                 score += 12

#         for concept in concepts:
#             if exact_word_present(
#                 concept,
#                 sentence
#             ):
#                 score += 12

#         if score > 0:
#             scored.append(
#                 (
#                     score,
#                     sentence
#                 )
#             )

#     scored.sort(
#         key=lambda x: x[0],
#         reverse=True
#     )

#     evidence = []
#     seen = set()
#     total = 0

#     for score, sentence in scored:
#         key = sentence.lower()

#         if key in seen:
#             continue

#         seen.add(key)
#         evidence.append(sentence)
#         total += len(sentence)

#         if total >= MAX_EVIDENCE_CHARS:
#             break

#     return clean_text(
#         " ".join(evidence)
#     )


# # ============================================================
# # OPENSEARCH INDEXING
# # ============================================================

# def index_document(
#     result,
#     page,
#     evidence,
#     question,
#     analysis
# ):
#     url = (
#         page.get("url")
#         or result.get("url", "")
#     )

#     document = {
#         "title": (
#             page.get("title")
#             or result.get(
#                 "title",
#                 ""
#             )
#         ),
#         "url": url,
#         "domain": get_domain(url),
#         "snippet": result.get(
#             "snippet",
#             ""
#         ),
#         "content": page.get(
#             "content",
#             ""
#         ),
#         "evidence": evidence,
#         "query": question,
#         "search_query": result.get(
#             "search_query",
#             ""
#         ),
#         "engine": result.get(
#             "engine",
#             ""
#         ),
#         "engine_rank": int(
#             result.get(
#                 "engine_rank",
#                 0
#             )
#             or 0
#         ),
#         "intent": analysis.get(
#             "intent",
#             ""
#         ),
#         "required_entities": analysis.get(
#             "entities",
#             []
#         ),
#         "required_concepts": analysis.get(
#             "concepts",
#             []
#         ),
#         "timestamp": datetime.now(
#             timezone.utc
#         ).isoformat(),
#     }

#     r = opensearch_request(
#         "POST",
#         f"/{INDEX_NAME}/_doc",
#         json=document
#     )

#     return bool(
#         r is not None
#         and r.ok
#     )


# # ============================================================
# # OPENSEARCH CACHE SEARCH
# # ============================================================

# def search_index_cache(
#     question,
#     analysis,
#     size=8
# ):
#     """
#     Cache is evidence enrichment only.

#     A historical document must contain every required entity and
#     every required concept before it can enter the candidate set.
#     It is never allowed to outrank a fresh source.
#     """

#     if not opensearch_ping():
#         return []

#     entities = analysis.get(
#         "entities",
#         []
#     )

#     concepts = analysis.get(
#         "concepts",
#         []
#     )

#     if not entities and not concepts:
#         return []

#     must = []

#     for entity in entities:
#         must.append({
#             "match_phrase": {
#                 "content": entity
#             }
#         })

#     for concept in concepts:
#         must.append({
#             "match_phrase": {
#                 "content": concept
#             }
#         })

#     body = {
#         "size": size,
#         "_source": [
#             "title",
#             "url",
#             "domain",
#             "snippet",
#             "evidence",
#             "content",
#             "query",
#             "search_query",
#             "engine",
#             "engine_rank",
#             "intent",
#             "required_entities",
#             "required_concepts",
#             "timestamp",
#         ],
#         "query": {
#             "bool": {
#                 "must": must,
#                 "should": [
#                     {
#                         "match": {
#                             "title": {
#                                 "query": question,
#                                 "operator": "and"
#                             }
#                         }
#                     },
#                     {
#                         "match": {
#                             "evidence": {
#                                 "query": question,
#                                 "operator": "and"
#                             }
#                         }
#                     },
#                     {
#                         "match": {
#                             "content": {
#                                 "query": question,
#                                 "operator": "and"
#                             }
#                         }
#                     },
#                 ],
#                 "minimum_should_match": 0,
#             }
#         },
#     }

#     r = opensearch_request(
#         "POST",
#         f"/{INDEX_NAME}/_search",
#         json=body
#     )

#     if not r or not r.ok:
#         return []

#     try:
#         hits = (
#             r.json()
#             .get(
#                 "hits",
#                 {}
#             )
#             .get(
#                 "hits",
#                 []
#             )
#         )
#     except Exception:
#         return []

#     results = []

#     for hit in hits:
#         source = hit.get(
#             "_source",
#             {}
#         )

#         source["cache"] = True
#         source["cache_score"] = float(
#             hit.get(
#                 "_score",
#                 0
#             )
#             or 0
#         )

#         # Re-validate against current question.
#         if entities:
#             if entity_coverage(
#                 source,
#                 entities
#             ) < 1.0:
#                 continue

#         if concepts:
#             if concept_coverage(
#                 source,
#                 concepts
#             ) < 1.0:
#                 continue

#         results.append(
#             source
#         )

#     return results


# # ============================================================
# # RESEARCH PIPELINE
# # ============================================================

# def research(
#     question,
#     client
# ):
#     global LAST_RESEARCH, LAST_QUERY

#     LAST_RESEARCH = []
#     LAST_QUERY = question

#     start = time.perf_counter()

#     print(
#         "\n[WEB SEARCH]"
#     )

#     print(
#         "Planning evidence retrieval with AI..."
#     )

#     analysis = plan_search_queries(
#         client,
#         question
#     )

#     if not analysis.get(
#         "queries"
#     ):
#         print(
#             "No usable search queries were generated."
#         )
#         return []

#     print(
#         "Searching and retrieving relevant sources..."
#     )

#     search_results = web_search(
#         question,
#         analysis
#     )

#     if not search_results:
#         print(
#             "No validated search results found."
#         )
#         return []

#     section(
#         "VALIDATED WEB SEARCH RESULTS"
#     )

#     for i, result in enumerate(
#         search_results,
#         1
#     ):
#         print(
#             f"\n[{i}] "
#             f"{result.get('title', '')}"
#         )

#         print(
#             f"    {result.get('url', '')}"
#         )

#         print(
#             f"    Score: "
#             f"{result.get('score', 0)}"
#         )

#         print(
#             "    Coverage: "
#             f"{result.get('query_coverage', 0)} | "
#             f"Entity: "
#             f"{result.get('entity_coverage', 0)} | "
#             f"Concept: "
#             f"{result.get('concept_coverage', 0)}"
#         )

#         print(
#             f"    Best query: "
#             f"{result.get('best_query', '')}"
#         )

#         print(
#             f"    Engine: "
#             f"{result.get('engine', '')} "
#             f"rank={result.get('engine_rank', '')}"
#         )

#         print(
#             f"    Search query: "
#             f"{result.get('search_query', '')}"
#         )

#         if result.get("snippet"):
#             print(
#                 f"    {result['snippet']}"
#             )

#     retrieved = []

#     fetch_count = min(
#         MAX_FETCH_RESULTS,
#         len(search_results)
#     )

#     print(
#         f"\nFetching up to "
#         f"{fetch_count} pages..."
#     )

#     for i, result in enumerate(
#         search_results[:fetch_count],
#         1
#     ):
#         url = normalize_url(
#             result.get(
#                 "url",
#                 ""
#             )
#         )

#         domain = get_domain(
#             url
#         )

#         print(
#             f"  [{i}/{fetch_count}] "
#             f"{domain} ",
#             end="",
#             flush=True
#         )

#         page = fetch_page(
#             url
#         )

#         if not page.get(
#             "success"
#         ):
#             print(
#                 "FAILED: "
#                 f"{page.get('error', 'unknown error')}"
#             )
#             continue

#         # Check actual fetched page.
#         page_probe = {
#             "title": page.get(
#                 "title",
#                 ""
#             ),
#             "snippet": page.get(
#                 "content",
#                 ""
#             )[:7000],
#             "content": page.get(
#                 "content",
#                 ""
#             ),
#             "url": page.get(
#                 "url",
#                 url
#             ),
#         }

#         allowed, reason = hard_relevance_gate(
#             page_probe,
#             analysis
#         )

#         if not allowed:
#             print(
#                 "REJECTED after fetch: "
#                 f"{reason}"
#             )
#             continue

#         evidence = extract_evidence(
#             page,
#             question,
#             analysis
#         )

#         print(
#             f"OK {page.get('elapsed', 0):.2f}s "
#             f"| evidence={len(evidence)}"
#         )

#         if not evidence:
#             print(
#                 "      Rejected: no usable evidence"
#             )
#             continue

#         record = {
#             **result,
#             "url": page.get(
#                 "url",
#                 url
#             ),
#             "title": (
#                 page.get(
#                     "title"
#                 )
#                 or result.get(
#                     "title",
#                     ""
#                 )
#             ),
#             "domain": get_domain(
#                 page.get(
#                     "url",
#                     url
#                 )
#             ),
#             "content": page.get(
#                 "content",
#                 ""
#             ),
#             "evidence": evidence,
#             "cache": False,
#         }

#         retrieved.append(
#             record
#         )

#         if index_document(
#             result,
#             page,
#             evidence,
#             question,
#             analysis
#         ):
#             print(
#                 "      Indexed in OpenSearch"
#             )

#     # Cache is enrichment only.
#     cache_results = search_index_cache(
#         question,
#         analysis,
#         size=8
#     )

#     merged = []
#     seen = set()

#     # Fresh sources first.
#     for item in (
#         retrieved + cache_results
#     ):
#         url = normalize_url(
#             item.get(
#                 "url",
#                 ""
#             )
#         )

#         key = canonical_url(
#             url
#         )

#         if not key or key in seen:
#             continue

#         seen.add(key)

#         item["url"] = url

#         merged.append(
#             item
#         )

#     final = []

#     for item in merged:
#         if analysis.get(
#             "entities"
#         ):
#             if entity_coverage(
#                 item,
#                 analysis["entities"]
#             ) < 1.0:
#                 continue

#         if analysis.get(
#             "concepts"
#         ):
#             if concept_coverage(
#                 item,
#                 analysis["concepts"]
#             ) < 1.0:
#                 continue

#         if not item.get(
#             "cache",
#             False
#         ):
#             # Fresh source score remains authoritative.
#             final.append(
#                 item
#             )
#         else:
#             # Cache is heavily discounted.
#             cache_score = float(
#                 item.get(
#                     "cache_score",
#                     0
#                 )
#                 or 0
#             )

#             item["score"] = round(
#                 min(
#                     cache_score * 0.20,
#                     35.0
#                 ),
#                 2
#             )

#             final.append(
#                 item
#             )

#     # Fresh sources first, then score.
#     final.sort(
#         key=lambda x: (
#             bool(
#                 x.get(
#                     "cache",
#                     False
#                 )
#             ),
#             -float(
#                 x.get(
#                     "score",
#                     0
#                 )
#             ),
#         )
#     )

#     LAST_RESEARCH = final[
#         :MAX_SEARCH_RESULTS
#     ]

#     elapsed = (
#         time.perf_counter()
#         - start
#     )

#     print(
#         f"\nRESEARCH COMPLETE | "
#         f"{len(LAST_RESEARCH)} sources | "
#         f"{elapsed:.2f}s"
#     )

#     return LAST_RESEARCH


# # ============================================================
# # LLM CONTEXT + ANSWER
# # ============================================================

# SYSTEM_PROMPT = """
# You are a research-oriented AI assistant.

# You receive evidence retrieved by a relevance-hardened web research system.

# Rules:
# 1. Do not invent facts.
# 2. Use supplied web evidence for factual claims.
# 3. Prefer fresh sources over cached historical sources.
# 4. Do not use a source merely because it contains generic keywords.
# 5. For company/person/executive questions, verify the specific entity
#    and requested role.
# 6. If sources disagree, explain the disagreement.
# 7. If evidence is insufficient, say it cannot be verified.
# 8. Include source URLs when web evidence supports the answer.
# 9. Keep answers concise unless the user asks for detail.
# """.strip()


# def build_evidence_context(records):
#     blocks = []
#     total = 0

#     for i, record in enumerate(
#         records,
#         1
#     ):
#         evidence = (
#             record.get(
#                 "evidence",
#                 ""
#             )
#             or record.get(
#                 "content",
#                 ""
#             )[:4000]
#         )

#         block = (
#             f"SOURCE {i}\n"
#             f"Title: {record.get('title', '')}\n"
#             f"URL: {record.get('url', '')}\n"
#             f"Domain: {record.get('domain', get_domain(record.get('url', '')))}\n"
#             f"Fresh web source: "
#             f"{not record.get('cache', False)}\n"
#             f"Search query: "
#             f"{record.get('search_query', '')}\n"
#             f"Score: "
#             f"{record.get('score', 0)}\n"
#             f"Evidence:\n{evidence}"
#         )

#         if total + len(block) > MAX_LLM_CONTEXT:
#             break

#         blocks.append(
#             block
#         )

#         total += len(block)

#     return "\n\n".join(
#         blocks
#     )


# def extract_content(chunk):
#     try:
#         choices = getattr(
#             chunk,
#             "choices",
#             None
#         )

#         if not choices:
#             return ""

#         choice = choices[0]

#         delta = getattr(
#             choice,
#             "delta",
#             None
#         )

#         if delta:
#             content = getattr(
#                 delta,
#                 "content",
#                 None
#             )

#             if content:
#                 return content

#         message = getattr(
#             choice,
#             "message",
#             None
#         )

#         if message:
#             content = getattr(
#                 message,
#                 "content",
#                 None
#             )

#             if content:
#                 return content

#     except Exception:
#         pass

#     return ""


# def send_message(
#     client,
#     history,
#     evidence_context=None
# ):
#     messages = list(
#         history
#     )

#     if evidence_context:
#         messages.append({
#             "role": "system",
#             "content": (
#                 "WEB RESEARCH EVIDENCE\n\n"
#                 "Use ONLY this evidence to verify factual claims.\n"
#                 "Do not invent facts.\n"
#                 "If evidence is insufficient, say so.\n"
#                 "Include source URLs when appropriate.\n\n"
#                 + evidence_context
#             ),
#         })

#     try:
#         response = client.chat.completions.create(
#             model=MODEL,
#             provider=AnyProvider,
#             messages=messages,
#             stream=True,
#         )

#         print(
#             "\nAI > ",
#             end="",
#             flush=True
#         )

#         answer = ""

#         for chunk in response:
#             content = extract_content(
#                 chunk
#             )

#             if content:
#                 answer += content
#                 print(
#                     content,
#                     end="",
#                     flush=True
#                 )

#         print()

#         return answer.strip()

#     except Exception as e:
#         print(
#             f"\nREQUEST ERROR: "
#             f"{type(e).__name__}: {e}"
#         )

#         return ""


# # ============================================================
# # CLI DISPLAY
# # ============================================================

# def show_sources():
#     section(
#         "RETRIEVED SOURCES"
#     )

#     if not LAST_RESEARCH:
#         print(
#             "No research sources available."
#         )
#         return

#     for i, item in enumerate(
#         LAST_RESEARCH,
#         1
#     ):
#         print(
#             f"\n[{i}] "
#             f"{item.get('title', '')}"
#         )

#         print(
#             f"    {item.get('url', '')}"
#         )

#         print(
#             f"    Domain: "
#             f"{item.get('domain', get_domain(item.get('url', '')))}"
#         )

#         print(
#             f"    Score: "
#             f"{item.get('score', 0)} "
#             f"| cache={item.get('cache', False)}"
#         )

#         if item.get(
#             "evidence"
#         ):
#             print(
#                 "    Evidence: "
#                 f"{item['evidence'][:1200]}"
#             )


# def show_research():
#     section(
#         "RETRIEVED WEBPAGE EVIDENCE"
#     )

#     if not LAST_RESEARCH:
#         print(
#             "No research data available."
#         )
#         return

#     for i, item in enumerate(
#         LAST_RESEARCH,
#         1
#     ):
#         print(
#             f"\n[{i}] "
#             f"{item.get('title', '')}"
#         )

#         print(
#             item.get(
#                 "url",
#                 ""
#             )
#         )

#         print(
#             item.get(
#                 "evidence",
#                 item.get(
#                     "content",
#                     ""
#                 )
#             )[:3000]
#         )

#         print(
#             "-" * WIDTH
#         )


# def open_url(url):
#     url = normalize_url(
#         url
#     )

#     if not url:
#         print(
#             "Invalid URL."
#         )
#         return

#     print(
#         f"\nFetching: {url}"
#     )

#     page = fetch_page(
#         url
#     )

#     if not page.get(
#         "success"
#     ):
#         print(
#             "Fetch failed: "
#             f"{page.get('error', 'unknown error')}"
#         )
#         return

#     section(
#         page.get(
#             "title",
#             "WEBPAGE"
#         )
#     )

#     print(
#         f"URL: "
#         f"{page.get('url', url)}\n"
#     )

#     print(
#         page.get(
#             "content",
#             ""
#         )[:10000]
#     )


# def show_history(history):
#     header(
#         "CONVERSATION HISTORY"
#     )

#     found = False

#     for message in history:
#         if message.get(
#             "role"
#         ) == "system":
#             continue

#         found = True

#         print(
#             f"\n{message.get('role', '').upper()}:"
#         )

#         print(
#             message.get(
#                 "content",
#                 ""
#             )
#         )

#         print(
#             "-" * WIDTH
#         )

#     if not found:
#         print(
#             "No conversation history."
#         )


# # ============================================================
# # CHAT
# # ============================================================

# def chat():
#     global LAST_RESEARCH

#     client = Client()

#     history = [
#         {
#             "role": "system",
#             "content": SYSTEM_PROMPT
#         }
#     ]

#     header(
#         "NIZAMI ANYPROVIDER GPT-4 OPENSEARCH WEB RESEARCH CLI"
#     )

#     print(
#         f"""
# Provider : AnyProvider
# Model    : {MODEL}

# WEB RESEARCH
# Engine   : Bing + DuckDuckGo
# Search DB: OpenSearch cache
# Fetcher  : requests
# Parser   : BeautifulSoup

# Relevance protection:
#   Query validation       : ON
#   Entity hard gate       : ON
#   Concept hard gate      : ON
#   Exact token matching   : ON
#   Best-query scoring     : ON
#   Page-level verification: ON
#   Fresh-source priority  : ON
#   Historical cache gate  : ON

# OpenSearch:
#   {OPENSEARCH_URL}

# Commands:
#   /exit              Exit
#   /clear             Clear conversation
#   /history           Show conversation
#   /model             Show model
#   /search <query>    Force web research
#   /sources           Show retrieved sources
#   /research          Show retrieved evidence
#   /open <url>        Fetch a specific webpage
#   /new               New conversation
#   /help              Show commands
# """
#     )

#     print(
#         "-" * WIDTH
#     )

#     while True:
#         try:
#             user_input = input(
#                 "\nYou > "
#             ).strip()

#         except (
#             KeyboardInterrupt,
#             EOFError
#         ):
#             print(
#                 "\nExiting..."
#             )
#             break

#         if not user_input:
#             continue

#         lower = user_input.lower()

#         if lower in {
#             "/exit",
#             "/quit",
#             "/q"
#         }:
#             print(
#                 "\nExiting..."
#             )
#             break

#         if lower in {
#             "/clear",
#             "/new"
#         }:
#             history = [
#                 {
#                     "role": "system",
#                     "content": SYSTEM_PROMPT
#                 }
#             ]

#             LAST_RESEARCH = []

#             print(
#                 "\nConversation cleared / "
#                 "new conversation started."
#             )

#             continue

#         if lower == "/history":
#             show_history(
#                 history
#             )
#             continue

#         if lower == "/sources":
#             show_sources()
#             continue

#         if lower == "/research":
#             show_research()
#             continue

#         if lower == "/model":
#             print(
#                 f"\nProvider : AnyProvider"
#                 f"\nModel    : {MODEL}"
#             )
#             continue

#         if lower == "/help":
#             print(
#                 """
# Commands:
#   /exit              Exit
#   /clear             Clear conversation
#   /history           Show conversation
#   /model             Show model
#   /search <query>    Force web research
#   /sources           Show retrieved sources
#   /research          Show retrieved evidence
#   /open <url>        Fetch a specific webpage
#   /new               New conversation
#   /help              Show commands
# """
#             )
#             continue

#         if lower.startswith(
#             "/open "
#         ):
#             open_url(
#                 user_input[
#                     len("/open "):
#                 ].strip()
#             )
#             continue

#         force_search = lower.startswith(
#             "/search"
#         )

#         raw_query = (
#             user_input[
#                 len("/search"):
#             ].strip()
#             if force_search
#             else user_input
#         )

#         needs_search = (
#             force_search
#             or should_search(
#                 user_input
#             )
#         )

#         evidence_context = None

#         if needs_search:
#             search_query = (
#                 normalize_query(
#                     raw_query
#                 )
#                 or user_input
#             )

#             records = research(
#                 search_query,
#                 client
#             )

#             evidence_context = (
#                 build_evidence_context(
#                     records
#                 )
#                 if records
#                 else None
#             )

#         history.append({
#             "role": "user",
#             "content": user_input
#         })

#         answer = send_message(
#             client,
#             history,
#             evidence_context
#         )

#         if answer:
#             history.append({
#                 "role": "assistant",
#                 "content": answer
#             })
#         else:
#             history.pop()


# # ============================================================
# # MAIN
# # ============================================================

# if __name__ == "__main__":
#     requests.packages.urllib3.disable_warnings()

#     header(
#         "NIZAMI GPT-4 / ANYPROVIDER / OPENSEARCH WEB RESEARCH"
#     )

#     AnyProvider = resolve_anyprovider()

#     print(
#         f"\nProvider class : {AnyProvider}"
#     )

#     print(
#         f"Model          : {MODEL}"
#     )

#     print(
#         "\nInitializing G4F client..."
#     )

#     try:
#         Client()
#         print(
#             "Client         : READY"
#         )

#     except Exception as e:
#         print(
#             f"Client         : FAILED "
#             f"({type(e).__name__}: {e})"
#         )
#         raise SystemExit(1)

#     print(
#         "\nConnecting to local OpenSearch..."
#     )

#     print(
#         f"\nOpenSearch URL:\n  "
#         f"{OPENSEARCH_URL}"
#     )

#     print(
#         f"\nOpenSearch directory:\n  "
#         f"{OPENSEARCH_DIR}"
#     )

#     if not start_opensearch():
#         print(
#             "\nOPENSEARCH STARTUP FAILED"
#         )
#         raise SystemExit(1)

#     show_opensearch_info()

#     print(
#         "\nInitializing OpenSearch index..."
#     )

#     if create_index():
#         print(
#             f"Index        : {INDEX_NAME}"
#         )
#         print(
#             "Index status : READY"
#         )
#     else:
#         print(
#             "Index status : FAILED"
#         )
#         raise SystemExit(1)

#     chat()





"""
NIZAMI WEB RESEARCH MCP SERVER
================================================================
An MCP server exposing relevance-hardened web research tools:

    - web_search    : search Bing + DuckDuckGo, rank + hard-gate results
    - fetch_page     : fetch a single URL fast (scrapling, falls back to requests)
    - crawl_site     : BFS-crawl a public site (scrapling) collecting evidence
    - search_cache   : query the OpenSearch evidence cache
    - analyze_query  : deterministic (no-LLM) entity/concept/intent extraction

Design notes (why it looks the way it does)
----------------------------------------------------------------
This used to be a standalone CLI chatbot that called a free/flaky g4f
"AnyProvider" GPT-4 endpoint to *plan* search queries before answering.
That LLM-planning step, the chat loop, and the CLI-only bits (/history,
/model, streaming answer printing, etc.) are gone: the calling model
(e.g. Claude, via MCP) is the planner now — it's a stronger and more
reliable reasoner than a free reverse-engineered API, so accuracy goes
up by removing that layer, not adding to it.

Everything else that mattered for *accuracy* is kept and still hard:
exact-token/phrase matching, entity/concept hard gates, fresh-source
priority over the OpenSearch cache, and score-threshold rejection.
web_search / fetch_page / crawl_site all accept optional
required_entities / required_concepts so the calling model can still
enforce "must mention Microsoft AND CEO" style gating -- it just does
so explicitly per call instead of via an internal planning prompt.

Fetching prefers `scrapling` (fast, has stealth headers / anti-bot
handling out of the box) and transparently falls back to
requests + BeautifulSoup if scrapling isn't installed or fails on a
given page.
"""

from __future__ import annotations

import html
import os
import re
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import base64

import requests
from bs4 import BeautifulSoup

from mcp.server.fastmcp import FastMCP

for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# OPTIONAL: scrapling (fast fetcher / anti-bot handling)
# ============================================================
try:
    from scrapling.fetchers import Fetcher as ScraplingFetcher
    SCRAPLING_AVAILABLE = True
except Exception:
    ScraplingFetcher = None
    SCRAPLING_AVAILABLE = False

# ============================================================
# CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parent

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "127.0.0.1")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_SCHEME = os.getenv("OPENSEARCH_SCHEME", "https")
OPENSEARCH_URL = f"{OPENSEARCH_SCHEME}://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}"
OPENSEARCH_USERNAME = os.getenv("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
INDEX_NAME = os.getenv("OPENSEARCH_INDEX", "nizami_web_research")
OPENSEARCH_TIMEOUT = 10

REQUEST_TIMEOUT = 20
MAX_SEARCH_RESULTS = 8
MAX_FETCH_RESULTS = 6
MAX_PAGE_CHARS = 14000
MAX_EVIDENCE_CHARS = 8000
MAX_TOTAL_SEARCH_CANDIDATES = 60
MAX_CRAWL_PAGES_HARD_CAP = 50

MIN_RESULT_SCORE = 30.0
MIN_QUERY_COVERAGE = 0.35

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but",
    "by", "can", "could", "did", "do", "does", "for", "from", "had", "has",
    "have", "how", "i", "if", "in", "into", "is", "it", "its", "may", "me",
    "more", "most", "my", "of", "on", "or", "our", "should", "that", "the",
    "their", "them", "there", "these", "they", "this", "to", "was", "we",
    "were", "what", "when", "where", "which", "who", "why", "will", "with",
    "would", "you", "your", "please", "tell", "find", "give", "show",
    "current", "currently", "today", "latest", "recent", "online", "web",
    "search", "look", "up", "know", "information", "info",
}

GENERIC_CONCEPTS = {
    "current", "currently", "today", "latest", "recent", "online", "web",
    "information", "info", "about", "details", "person", "company",
}

ROLE_TERMS = {
    "ceo": "CEO", "chief executive officer": "CEO",
    "founder": "founder", "co-founder": "co-founder",
    "owner": "owner", "director": "director", "president": "president",
    "chairman": "chairman", "chairperson": "chairperson",
    "managing director": "managing director", "executive": "executive",
    "cto": "CTO", "cfo": "CFO", "coo": "COO", "head": "head",
    "leader": "leader", "leadership": "leadership",
}

QUESTION_INTENT_TERMS = [
    ("how much", "quantity"), ("how many", "quantity"),
    ("who is", "identity"), ("who", "identity"),
    ("what is", "fact"), ("what", "fact"),
    ("when", "date"), ("where", "location"),
    ("price", "price"), ("cost", "price"),
    ("latest", "current_events"), ("recent", "current_events"),
    ("news", "current_events"), ("compare", "comparison"),
    ("difference", "comparison"), ("review", "review"),
]

TRUSTED_DOMAIN_SCORES = {
    "microsoft.com": 100, "apple.com": 100, "google.com": 100,
    "amazon.com": 95, "meta.com": 95, "openai.com": 100, "ibm.com": 95,
    "intel.com": 95, "nvidia.com": 95, "oracle.com": 95,
    "linkedin.com": 72, "reuters.com": 72, "apnews.com": 70,
    "bbc.com": 68, "bbc.co.uk": 68, "bloomberg.com": 68,
    "nytimes.com": 65, "wsj.com": 65, "forbes.com": 58, "cnbc.com": 58,
    "theguardian.com": 55, "wikipedia.org": 35,
}

LOW_AUTHORITY_DOMAINS = {"electricalvolt.com", "current.com", "example.com"}

KNOWN_COMPANIES = {
    "microsoft": "microsoft.com", "apple": "apple.com", "google": "google.com",
    "amazon": "amazon.com", "meta": "meta.com", "openai": "openai.com",
    "nvidia": "nvidia.com", "ibm": "ibm.com", "intel": "intel.com",
    "oracle": "oracle.com", "samsung": "samsung.com", "tesla": "tesla.com",
    "hyundai": "hyundai.com", "toyota": "toyota.com",
}

COMPANY_ALIASES = {
    "microsoft": {"microsoft", "msft"},
    "apple": {"apple", "apple inc"},
    "google": {"google", "alphabet"},
    "meta": {"meta", "facebook"},
    "openai": {"openai"},
    "amazon": {"amazon", "amazon.com"},
    "nvidia": {"nvidia"}, "ibm": {"ibm"}, "intel": {"intel"}, "oracle": {"oracle"},
}

CONCEPT_ALIASES = {
    "ceo": {"ceo", "chief executive officer", "chief executive"},
    "founder": {"founder", "co-founder", "cofounder", "founded by"},
    "director": {"director", "managing director"},
    "president": {"president"},
    "chairman": {"chairman", "chairperson"},
    "leadership": {"leadership", "management", "executive team", "leadership team"},
    "executive": {"executive", "executive leadership"},
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

OPENSEARCH_AVAILABLE: Optional[bool] = None  # lazily probed


# ============================================================
# TEXT / URL HELPERS
# ============================================================

def clean_text(text: Any) -> str:
    if not text:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+(?:[._'-][a-z0-9]+)*", text.lower())


def meaningful_tokens(text: str) -> set[str]:
    return {t for t in tokenize(text) if len(t) >= 3 and t not in STOPWORDS}


def exact_word_present(term: str, text: str) -> bool:
    term = clean_text(term).lower()
    text = clean_text(text).lower()
    if not term:
        return False
    return bool(re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", text))


def phrase_tokens(term: str) -> list[str]:
    return [t for t in tokenize(term) if len(t) >= 2 and t not in STOPWORDS]


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = html.unescape(url).strip()

    if "bing.com/ck/a" in url:
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            encoded = params.get("u", [])
            if encoded:
                value = encoded[0]
                if value.startswith("a1"):
                    decoded = base64.b64decode(value[2:] + "==").decode("utf-8", errors="ignore")
                    if decoded.startswith(("http://", "https://")):
                        url = decoded
        except Exception:
            pass

    try:
        return urlparse(url)._replace(fragment="").geturl()
    except Exception:
        return url


def canonical_url(url: str) -> str:
    url = normalize_url(url)
    if not url:
        return ""
    try:
        p = urlparse(url)
        netloc = p.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = p.path.rstrip("/") or "/"
        ignored = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid"}
        parts = []
        for key, values in parse_qs(p.query, keep_blank_values=True).items():
            if key.lower() in ignored:
                continue
            for v in values:
                parts.append(f"{key}={v}")
        query = "&".join(sorted(parts))
        return f"{p.scheme.lower()}://{netloc}{path}" + (f"?{query}" if query else "")
    except Exception:
        return url


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def dedupe_strings(items: list[str]) -> list[str]:
    out, seen = [], set()
    for item in items:
        item = clean_text(item)
        if not item:
            continue
        key = item.lower()
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


# ============================================================
# QUERY ANALYSIS (deterministic, no LLM)
# ============================================================

def normalize_query(user_input: str) -> str:
    query = clean_text(user_input)
    prefixes = [
        r"^/search\s+", r"^search\s+the\s+web\s+for\s+", r"^search\s+the\s+web\s+",
        r"^web\s+search\s+for\s+", r"^web\s+search\s+", r"^search\s+online\s+for\s+",
        r"^search\s+online\s+", r"^google\s+this\s+", r"^look\s+this\s+up\s+",
        r"^look\s+it\s+up\s+", r"^do\s+a\s+web\s+search\s+and\s+find\s+",
        r"^do\s+a\s+web\s+search\s+", r"^find\s+",
    ]
    for pattern in prefixes:
        new_query = re.sub(pattern, "", query, flags=re.IGNORECASE)
        if new_query != query:
            query = new_query
            break
    return clean_text(query.strip(" ?!."))


def detect_intent(question: str) -> str:
    lower = question.lower()
    for term, intent in QUESTION_INTENT_TERMS:
        if re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", lower):
            return intent
    return "general_fact"


def extract_capitalized_entities(question: str) -> list[str]:
    entities = []
    for match in re.findall(r"\b[A-Z][A-Za-z0-9&.-]*(?:\s+[A-Z][A-Za-z0-9&.-]*){0,4}", question):
        candidate = clean_text(match).strip(" ,.!?:")
        if not candidate:
            continue
        if candidate.lower() in {"who", "what", "when", "where", "why", "how", "tell", "find", "search", "please"}:
            continue
        if len(tokenize(candidate)) == 1 and candidate.lower() in ROLE_TERMS:
            continue
        if len(candidate) >= 3:
            entities.append(candidate)
    return dedupe_strings(entities)


def local_query_analysis(question: str) -> dict:
    """Deterministic entity/concept/intent extraction. No LLM involved."""
    q = normalize_query(question)
    entities: list[str] = []
    concepts: list[str] = []

    for name in KNOWN_COMPANIES:
        if exact_word_present(name, q):
            entities.append(name)

    for entity in extract_capitalized_entities(question):
        if entity.lower() not in {x.lower() for x in entities}:
            entities.append(entity)

    for role in sorted(ROLE_TERMS, key=len, reverse=True):
        if exact_word_present(role, q):
            concepts.append(ROLE_TERMS[role])

    role_names = {r.lower() for r in ROLE_TERMS}
    entities = [e for e in entities if e.lower() not in role_names]
    concepts = [c for c in dedupe_strings(concepts) if c.lower() not in GENERIC_CONCEPTS]

    return {
        "intent": detect_intent(q),
        "entities": entities[:8],
        "concepts": concepts[:8],
    }


def query_contains_entity(query: str, entity: str) -> bool:
    if exact_word_present(entity, query):
        return True
    for alias in COMPANY_ALIASES.get(entity.lower(), {entity.lower()}):
        if exact_word_present(alias, query):
            return True
    return False


def query_contains_concept(query: str, concept: str) -> bool:
    if exact_word_present(concept, query):
        return True
    tokens = phrase_tokens(concept)
    if len(tokens) >= 2:
        q_tokens = set(tokenize(query))
        matched = sum(1 for t in tokens if t in q_tokens)
        return matched >= max(1, len(tokens) // 2)
    return False


def concept_matches_text(concept: str, text: str) -> bool:
    if exact_word_present(concept, text):
        return True
    return any(exact_word_present(a, text) for a in CONCEPT_ALIASES.get(concept.lower(), {concept.lower()}))


def get_domain_for_entity(entity: str) -> str:
    return KNOWN_COMPANIES.get(entity.lower(), "")


def build_queries(question: str, entities: list[str], concepts: list[str], intent: str, limit: int = 5) -> list[str]:
    """Deterministic query expansion around required entities/concepts."""
    q = clean_text(question)
    queries = []

    if entities:
        entity = entities[0]
        if concepts:
            concept = concepts[0]
            queries.append(f'"{entity}" "{concept}"')
            queries.append(f"{entity} {concept}")
            domain = get_domain_for_entity(entity)
            if domain:
                queries.append(f"site:{domain} {entity} {concept}")
        else:
            queries.append(f'"{entity}"')

    if not queries:
        queries.append(q)

    if intent in {"current_events", "date"}:
        queries.append(f"{q} {datetime.now().year}")

    valid = [c for c in queries if _validate_query(c, entities, concepts)]
    if _validate_query(q, entities, concepts):
        valid.append(q)

    return dedupe_strings(valid)[:limit]


def _validate_query(candidate: str, required_entities: list[str], required_concepts: list[str]) -> bool:
    candidate = clean_text(candidate)
    if len(candidate) < 3:
        return False
    for entity in required_entities:
        if not query_contains_entity(candidate, entity):
            return False
    if required_concepts:
        if not any(query_contains_concept(candidate, c) for c in required_concepts):
            return False
    return True


# ============================================================
# SEARCH ENGINES
# ============================================================

def bing_search(query: str, max_results: int = 8) -> list[dict]:
    try:
        r = SESSION.get(
            "https://www.bing.com/search",
            params={"q": query, "count": max_results},
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                     "Accept-Language": "en-US,en;q=0.9"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    for item in soup.select("li.b_algo"):
        link = item.select_one("h2 a")
        if not link:
            continue
        href = normalize_url(link.get("href", ""))
        if not href:
            continue
        title = clean_text(link.get_text(" ", strip=True))
        snippet_el = item.select_one(".b_caption p")
        snippet = clean_text(snippet_el.get_text(" ", strip=True)) if snippet_el else ""
        results.append({
            "title": title, "url": href, "snippet": snippet,
            "engine": "Bing", "engine_rank": len(results) + 1, "search_query": query,
        })
        if len(results) >= max_results:
            break
    return results


def duckduckgo_search(query: str, max_results: int = 8) -> list[dict]:
    try:
        r = SESSION.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    for item in soup.select(".result"):
        link = item.select_one(".result__a")
        if not link:
            continue
        href = normalize_url(link.get("href", ""))
        if not href:
            continue
        title = clean_text(link.get_text(" ", strip=True))
        snippet_el = item.select_one(".result__snippet")
        snippet = clean_text(snippet_el.get_text(" ", strip=True)) if snippet_el else ""
        results.append({
            "title": title, "url": href, "snippet": snippet,
            "engine": "DuckDuckGo", "engine_rank": len(results) + 1, "search_query": query,
        })
        if len(results) >= max_results:
            break
    return results


# ============================================================
# RELEVANCE ENGINE
# ============================================================

def text_for_result(result: dict) -> str:
    return " ".join([
        result.get("title", ""), result.get("snippet", ""),
        result.get("content", ""), result.get("evidence", ""), result.get("url", ""),
    ])


def query_coverage(query: str, result: dict) -> float:
    q_tokens = meaningful_tokens(query)
    if not q_tokens:
        return 0.0
    haystack = set(tokenize(result.get("title", ""))) | set(tokenize(result.get("snippet", ""))) \
        | set(tokenize(result.get("content", ""))) | set(tokenize(result.get("url", "")))
    matched = sum(1 for t in q_tokens if t in haystack)
    return matched / len(q_tokens)


def entity_coverage(result: dict, entities: list[str]) -> float:
    if not entities:
        return 1.0
    text = text_for_result(result)
    matched = sum(1 for e in entities if exact_word_present(e, text) or query_contains_entity(text, e))
    return matched / len(entities)


def concept_coverage(result: dict, concepts: list[str]) -> float:
    if not concepts:
        return 1.0
    text = text_for_result(result)
    matched = sum(1 for c in concepts if concept_matches_text(c, text))
    return matched / len(concepts)


def domain_authority(domain: str) -> int:
    if domain in TRUSTED_DOMAIN_SCORES:
        return TRUSTED_DOMAIN_SCORES[domain]
    for root in TRUSTED_DOMAIN_SCORES:
        if domain.endswith("." + root):
            return 45
    if domain in LOW_AUTHORITY_DOMAINS:
        return -80
    return 0


def best_query_coverage(result: dict, queries: list[str]) -> tuple[float, str]:
    if not queries:
        return 0.0, ""
    scored = [(query_coverage(q, result), q) for q in queries]
    return max(scored, key=lambda x: x[0])


def hard_relevance_gate(result: dict, entities: list[str], concepts: list[str], queries: list[str]) -> tuple[bool, str]:
    ecover = entity_coverage(result, entities)
    ccover = concept_coverage(result, concepts)
    coverage, _ = best_query_coverage(result, queries)

    if entities and ecover < 1.0:
        return False, "missing required entity"
    if concepts and ccover < 1.0:
        return False, "missing required concept"
    if coverage < MIN_QUERY_COVERAGE:
        if not (entities and concepts and ecover == 1.0 and ccover == 1.0):
            return False, "low query coverage"
    return True, "ok"


def rank_results(results: list[dict], entities: list[str], concepts: list[str], queries: list[str]) -> list[dict]:
    ranked = []
    for result in results:
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        url = result.get("url", "")
        domain = get_domain(url)

        allowed, reason = hard_relevance_gate(result, entities, concepts, queries)
        if not allowed:
            result["rejected_reason"] = reason
            continue

        ecover = entity_coverage(result, entities)
        ccover = concept_coverage(result, concepts)
        best_coverage, best_query = best_query_coverage(result, queries)

        title_lower, snippet_lower = title.lower(), snippet.lower()
        exact_phrase = 0
        for q in queries:
            phrase = clean_text(q).lower()
            if not phrase:
                continue
            if phrase in title_lower:
                exact_phrase = max(exact_phrase, 2)
            elif phrase in snippet_lower:
                exact_phrase = max(exact_phrase, 1)

        score = 0.0
        score += best_coverage * 50
        score += ecover * 40
        score += ccover * 30
        score += exact_phrase * 12
        score += min(domain_authority(domain), 100) * 0.25

        try:
            engine_rank = int(result.get("engine_rank", 10) or 10)
        except Exception:
            engine_rank = 10
        score += max(0.0, 10.0 - engine_rank)

        for entity in entities:
            if query_contains_entity(title, entity):
                score += 22
        for concept in concepts:
            if exact_word_present(concept, title):
                score += 18

        lower_url = url.lower()
        if any(p in lower_url for p in ("/leadership", "/about", "/company", "/management", "/executive", "/people", "/team")):
            score += 8
        if domain in LOW_AUTHORITY_DOMAINS:
            score -= 80

        result["best_query"] = best_query
        result["query_coverage"] = round(best_coverage, 3)
        result["entity_coverage"] = round(ecover, 3)
        result["concept_coverage"] = round(ccover, 3)
        result["score"] = round(score, 2)

        if score < MIN_RESULT_SCORE:
            result["rejected_reason"] = f"score below threshold ({score:.2f})"
            continue

        ranked.append(result)

    ranked.sort(key=lambda x: (-float(x.get("score", 0)), x.get("engine_rank", 999)))
    return ranked


# ============================================================
# FETCHING (scrapling first, requests fallback)
# ============================================================

def _clean_html_to_page(url: str, html_content: str, status: int) -> dict:
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()

    title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""
    metadata = []
    for meta in soup.find_all("meta"):
        key = (meta.get("name", "") or meta.get("property", "")).lower()
        content = meta.get("content", "")
        if key in {"description", "og:description", "author", "og:title"} and content:
            metadata.append(clean_text(content))

    body = clean_text(soup.get_text(" ", strip=True))
    content = clean_text(" ".join(metadata) + " " + body)[:MAX_PAGE_CHARS]

    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"])
        href = normalize_url(href)
        if href.startswith(("http://", "https://")):
            links.append(href)

    return {
        "success": True, "url": url, "title": title, "content": content,
        "status": status, "links": dedupe_strings(links)[:200],
    }


def fetch_page(url: str) -> dict:
    """Fetch a page. Tries scrapling first (fast + stealth), falls back to requests."""
    start = time.perf_counter()

    if SCRAPLING_AVAILABLE:
        try:
            page = ScraplingFetcher.get(url, stealthy_headers=True, follow_redirects=True, timeout=REQUEST_TIMEOUT)
            status = getattr(page, "status", 200)
            if status and status < 400:
                html_content = getattr(page, "html_content", None) or str(page)
                result = _clean_html_to_page(url, html_content, status)
                result["fetched_via"] = "scrapling"
                result["elapsed"] = round(time.perf_counter() - start, 3)
                return result
        except Exception:
            pass  # fall through to requests

    try:
        r = SESSION.get(
            url,
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                     "Accept-Language": "en-US,en;q=0.9"},
            timeout=REQUEST_TIMEOUT, allow_redirects=True,
        )
        r.raise_for_status()
    except Exception as e:
        return {"success": False, "url": url, "error": f"{type(e).__name__}: {e}",
                "content": "", "elapsed": round(time.perf_counter() - start, 3)}

    content_type = r.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        return {"success": False, "url": r.url, "error": f"Unsupported content type: {content_type}",
                "content": "", "elapsed": round(time.perf_counter() - start, 3)}

    result = _clean_html_to_page(r.url, r.text, r.status_code)
    result["fetched_via"] = "requests"
    result["elapsed"] = round(time.perf_counter() - start, 3)
    return result


# ============================================================
# EVIDENCE EXTRACTION
# ============================================================

def extract_evidence(content: str, question: str, entities: list[str], concepts: list[str]) -> str:
    if not content:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", content)
    q_tokens = meaningful_tokens(question)

    scored = []
    for sentence in sentences:
        sentence = clean_text(sentence)
        if len(sentence) < 35:
            continue
        score = 0.0
        for token in q_tokens:
            if exact_word_present(token, sentence):
                score += 1.5
        for entity in entities:
            if query_contains_entity(sentence, entity):
                score += 12
        for concept in concepts:
            if exact_word_present(concept, sentence):
                score += 12
        if score > 0:
            scored.append((score, sentence))

    scored.sort(key=lambda x: x[0], reverse=True)
    evidence, seen, total = [], set(), 0
    for score, sentence in scored:
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        evidence.append(sentence)
        total += len(sentence)
        if total >= MAX_EVIDENCE_CHARS:
            break
    return clean_text(" ".join(evidence))


# ============================================================
# OPENSEARCH CACHE (lazy: never blocks tool calls if unavailable)
# ============================================================

def _opensearch_auth():
    return (OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD) if OPENSEARCH_PASSWORD else None


def _opensearch_request(method: str, path: str, **kwargs):
    url = OPENSEARCH_URL.rstrip("/") + "/" + path.lstrip("/")
    kwargs.setdefault("timeout", OPENSEARCH_TIMEOUT)
    kwargs.setdefault("verify", False)
    auth = _opensearch_auth()
    if auth:
        kwargs.setdefault("auth", auth)
    try:
        return SESSION.request(method, url, **kwargs)
    except Exception:
        return None


def opensearch_ping() -> bool:
    r = _opensearch_request("GET", "/")
    return bool(r is not None and r.ok)


def ensure_opensearch_index() -> bool:
    """Best-effort: probe + create index once. Never raises."""
    global OPENSEARCH_AVAILABLE
    if OPENSEARCH_AVAILABLE is not None:
        return OPENSEARCH_AVAILABLE

    if not opensearch_ping():
        OPENSEARCH_AVAILABLE = False
        return False

    mapping = {
        "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
        "mappings": {"properties": {
            "title": {"type": "text"}, "url": {"type": "keyword"}, "domain": {"type": "keyword"},
            "snippet": {"type": "text"}, "content": {"type": "text"}, "evidence": {"type": "text"},
            "query": {"type": "text"}, "search_query": {"type": "text"}, "engine": {"type": "keyword"},
            "engine_rank": {"type": "integer"}, "intent": {"type": "keyword"},
            "required_entities": {"type": "keyword"}, "required_concepts": {"type": "keyword"},
            "timestamp": {"type": "date"},
        }},
    }
    head = _opensearch_request("HEAD", f"/{INDEX_NAME}")
    if head is not None and head.status_code == 200:
        OPENSEARCH_AVAILABLE = True
        return True

    r = _opensearch_request("PUT", f"/{INDEX_NAME}", json=mapping)
    OPENSEARCH_AVAILABLE = bool(r is not None and r.status_code in (200, 201))
    return OPENSEARCH_AVAILABLE


def index_document(url: str, title: str, snippet: str, content: str, evidence: str,
                    question: str, search_query: str, engine: str, engine_rank: int,
                    intent: str, entities: list[str], concepts: list[str]) -> bool:
    if not ensure_opensearch_index():
        return False
    document = {
        "title": title, "url": url, "domain": get_domain(url), "snippet": snippet,
        "content": content, "evidence": evidence, "query": question, "search_query": search_query,
        "engine": engine, "engine_rank": int(engine_rank or 0), "intent": intent,
        "required_entities": entities, "required_concepts": concepts,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    r = _opensearch_request("POST", f"/{INDEX_NAME}/_doc", json=document)
    return bool(r is not None and r.ok)


def search_index_cache(question: str, entities: list[str], concepts: list[str], size: int = 8) -> list[dict]:
    if not ensure_opensearch_index():
        return []
    if not entities and not concepts:
        return []

    must = [{"match_phrase": {"content": e}} for e in entities] + \
           [{"match_phrase": {"content": c}} for c in concepts]

    body = {
        "size": size,
        "_source": ["title", "url", "domain", "snippet", "evidence", "content", "query",
                    "search_query", "engine", "engine_rank", "intent", "required_entities",
                    "required_concepts", "timestamp"],
        "query": {"bool": {
            "must": must,
            "should": [
                {"match": {"title": {"query": question, "operator": "and"}}},
                {"match": {"evidence": {"query": question, "operator": "and"}}},
                {"match": {"content": {"query": question, "operator": "and"}}},
            ],
            "minimum_should_match": 0,
        }},
    }

    r = _opensearch_request("POST", f"/{INDEX_NAME}/_search", json=body)
    if not r or not r.ok:
        return []
    try:
        hits = r.json().get("hits", {}).get("hits", [])
    except Exception:
        return []

    results = []
    for hit in hits:
        source = hit.get("_source", {})
        source["cache"] = True
        source["cache_score"] = float(hit.get("_score", 0) or 0)
        if entities and entity_coverage(source, entities) < 1.0:
            continue
        if concepts and concept_coverage(source, concepts) < 1.0:
            continue
        results.append(source)
    return results


# ============================================================
# MCP SERVER
# ============================================================

mcp = FastMCP("nizami-web-research")


@mcp.tool()
def analyze_query(question: str) -> dict:
    """
    Deterministically extract intent, required named entities, and required
    concepts (roles like CEO/founder/director, etc.) from a question. No LLM
    involved -- pure rule-based extraction. Call this first if you want a
    quick, cheap default set of entities/concepts to pass into web_search /
    fetch_page / crawl_site for hard relevance gating. You can also just
    supply your own entities/concepts based on your own understanding of the
    question, which is usually more accurate.
    """
    return local_query_analysis(question)


@mcp.tool()
def web_search(
    query: str,
    required_entities: Optional[list[str]] = None,
    required_concepts: Optional[list[str]] = None,
    max_results: int = 8,
) -> dict:
    """
    Search the public web (Bing + DuckDuckGo) for a query and return a
    relevance-hardened, ranked list of results.

    required_entities: named entities (e.g. company/person names) that MUST
        appear in a result for it to be kept. If omitted, entities are
        auto-derived from `query` via deterministic extraction.
    required_concepts: attributes/roles (e.g. "CEO", "founder") that MUST
        appear (via alias matching) in a result for it to be kept. If
        omitted, concepts are auto-derived from `query`.
    max_results: cap on how many ranked results to return (default 8).

    Results below a minimum relevance score, missing a required entity, or
    missing a required concept are dropped entirely rather than merely
    down-ranked. For best accuracy, pass explicit required_entities /
    required_concepts based on what you know the user actually asked for.
    """
    max_results = max(1, min(int(max_results or 8), MAX_SEARCH_RESULTS))
    local = local_query_analysis(query)
    entities = dedupe_strings(required_entities) if required_entities else local["entities"]
    concepts = dedupe_strings(required_concepts) if required_concepts else local["concepts"]
    queries = build_queries(query, entities, concepts, local["intent"]) or [query]

    all_results: list[dict] = []
    seen_urls: set[str] = set()

    for search_query in queries:
        for engine in (bing_search, duckduckgo_search):
            for result in engine(search_query):
                url = normalize_url(result.get("url", ""))
                key = canonical_url(url)
                if not key:
                    continue
                domain = get_domain(url)
                if domain in {"bing.com", "duckduckgo.com"}:
                    continue
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                all_results.append(result)
                if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
                    break
            if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
                break
        if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
            break

    ranked = rank_results(all_results, entities, concepts, queries)[:max_results]

    return {
        "query": query,
        "queries_used": queries,
        "required_entities": entities,
        "required_concepts": concepts,
        "results": ranked,
        "result_count": len(ranked),
    }


@mcp.tool()
def fetch_page_tool(
    url: str,
    question: Optional[str] = None,
    required_entities: Optional[list[str]] = None,
    required_concepts: Optional[list[str]] = None,
    index_in_cache: bool = True,
) -> dict:
    """
    Fetch a single public web page and return its cleaned text content
    (title, meta description, body text). Uses `scrapling` for speed and
    anti-bot resilience when available, otherwise falls back to
    requests + BeautifulSoup automatically.

    question: if provided, the most relevant sentences to this question are
        extracted separately into an `evidence` field (cheaper to read than
        the full page content).
    required_entities / required_concepts: if provided, the fetched page is
        checked against them; if it fails the gate, `passed_relevance_gate`
        will be False (page content is still returned so you can judge for
        yourself).
    index_in_cache: if True (default) and OpenSearch is reachable, store the
        page + evidence in the cache for future search_cache calls.
    """
    url = normalize_url(url)
    if not url:
        return {"success": False, "error": "invalid url"}

    page = fetch_page(url)
    if not page.get("success"):
        return page

    entities = dedupe_strings(required_entities) if required_entities else []
    concepts = dedupe_strings(required_concepts) if required_concepts else []

    passed = True
    reason = "ok"
    if entities or concepts:
        probe = {"title": page.get("title", ""), "snippet": page.get("content", "")[:7000],
                  "content": page.get("content", ""), "url": page.get("url", url)}
        passed, reason = hard_relevance_gate(probe, entities, concepts, [question or ""])

    evidence = ""
    if question:
        evidence = extract_evidence(page.get("content", ""), question, entities, concepts)

    if index_in_cache:
        index_document(
            url=page.get("url", url), title=page.get("title", ""), snippet="",
            content=page.get("content", ""), evidence=evidence, question=question or "",
            search_query="", engine="direct_fetch", engine_rank=0,
            intent=local_query_analysis(question)["intent"] if question else "",
            entities=entities, concepts=concepts,
        )

    return {
        "success": True,
        "url": page.get("url", url),
        "title": page.get("title", ""),
        "content": page.get("content", ""),
        "evidence": evidence,
        "fetched_via": page.get("fetched_via"),
        "elapsed_seconds": page.get("elapsed"),
        "passed_relevance_gate": passed,
        "relevance_reason": reason,
        "links_found": len(page.get("links", [])),
    }


@mcp.tool()
def crawl_site(
    start_url: str,
    question: Optional[str] = None,
    required_entities: Optional[list[str]] = None,
    required_concepts: Optional[list[str]] = None,
    max_pages: int = 15,
    same_domain: bool = True,
    index_in_cache: bool = True,
) -> dict:
    """
    Breadth-first crawl a public site starting from `start_url`, following
    links (same-domain only by default), fetching each page via `scrapling`
    (falls back to requests), and returning the pages most relevant to
    `question` / required_entities / required_concepts.

    Use this instead of repeated fetch_page_tool calls when you need to
    explore a site (e.g. a company's /about, /leadership, /team pages) to
    find one specific fact, rather than fetching a URL you already know.

    max_pages is capped at 50 for politeness; a short delay is added
    between requests.
    """
    start_url = normalize_url(start_url)
    if not start_url:
        return {"success": False, "error": "invalid start_url"}

    max_pages = max(1, min(int(max_pages or 15), MAX_CRAWL_PAGES_HARD_CAP))
    entities = dedupe_strings(required_entities) if required_entities else []
    concepts = dedupe_strings(required_concepts) if required_concepts else []
    root_domain = get_domain(start_url)

    visited: set[str] = set()
    queue: deque[str] = deque([start_url])
    pages: list[dict] = []
    errors: list[dict] = []

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        key = canonical_url(url)
        if key in visited:
            continue
        visited.add(key)

        page = fetch_page(url)
        if not page.get("success"):
            errors.append({"url": url, "error": page.get("error", "unknown error")})
            continue

        evidence = ""
        if question or entities or concepts:
            evidence = extract_evidence(page.get("content", ""), question or "", entities, concepts)

        record = {
            "url": page.get("url", url),
            "title": page.get("title", ""),
            "content": page.get("content", "")[:4000],
            "evidence": evidence,
            "fetched_via": page.get("fetched_via"),
        }
        pages.append(record)

        if index_in_cache:
            index_document(
                url=page.get("url", url), title=page.get("title", ""), snippet="",
                content=page.get("content", ""), evidence=evidence, question=question or "",
                search_query="", engine="crawl", engine_rank=0,
                intent=local_query_analysis(question)["intent"] if question else "",
                entities=entities, concepts=concepts,
            )

        for link in page.get("links", []):
            link_key = canonical_url(link)
            if link_key in visited:
                continue
            if same_domain and get_domain(link) != root_domain:
                continue
            queue.append(link)

        time.sleep(0.4)  # politeness delay

    # Rank crawled pages by relevance if we have something to score against.
    if question or entities or concepts:
        scored = []
        for p in pages:
            probe = {"title": p["title"], "snippet": p["content"][:2000], "content": p["content"], "evidence": p["evidence"]}
            ecover = entity_coverage(probe, entities)
            ccover = concept_coverage(probe, concepts)
            qcover = query_coverage(question or "", probe)
            p["entity_coverage"] = round(ecover, 3)
            p["concept_coverage"] = round(ccover, 3)
            p["query_coverage"] = round(qcover, 3)
            p["relevance_score"] = round(ecover * 40 + ccover * 30 + qcover * 30, 2)
            scored.append(p)
        scored.sort(key=lambda x: -x["relevance_score"])
        pages = scored

    return {
        "success": True,
        "start_url": start_url,
        "pages_crawled": len(pages),
        "pages_failed": len(errors),
        "errors": errors,
        "pages": pages,
    }


@mcp.tool()
def search_cache(
    question: str,
    required_entities: Optional[list[str]] = None,
    required_concepts: Optional[list[str]] = None,
    max_results: int = 8,
) -> dict:
    """
    Search previously-fetched/crawled pages stored in the OpenSearch
    evidence cache. This is historical evidence enrichment only -- always
    prefer fresh web_search / fetch_page_tool / crawl_site results over the
    cache for anything time-sensitive. Returns an empty result set (not an
    error) if OpenSearch is unreachable or the index is empty.
    """
    local = local_query_analysis(question)
    entities = dedupe_strings(required_entities) if required_entities else local["entities"]
    concepts = dedupe_strings(required_concepts) if required_concepts else local["concepts"]

    results = search_index_cache(question, entities, concepts, size=max(1, min(int(max_results or 8), 20)))
    return {
        "question": question,
        "required_entities": entities,
        "required_concepts": concepts,
        "opensearch_reachable": ensure_opensearch_index(),
        "results": results,
        "result_count": len(results),
    }


if __name__ == "__main__":
    mcp.run()