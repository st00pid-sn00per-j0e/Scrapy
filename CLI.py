"""Nizami Web Research - standalone terminal CLI.

Run: python nizami_cli.py
Then type questions directly in the terminal.
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

try:
    from mcp.server.fastmcp import FastMCP
except Exception:
    class FastMCP:
        def __init__(self, *args, **kwargs):
            pass
        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator
        def run(self, *args, **kwargs):
            raise RuntimeError("MCP package is not installed. Install 'mcp' only if you also want MCP mode.")

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




# ============================================================
# STANDALONE TERMINAL CHAT
# ============================================================

import textwrap

CLI_SYSTEM_PROMPT = """
You are Nizami, a concise research-oriented AI assistant running in a terminal.

Rules:
1. Use supplied web evidence for factual claims when evidence is provided.
2. Prefer fresh web sources over cached evidence.
3. Never invent facts or pretend a claim was verified when it was not.
4. If sources disagree, explain the disagreement briefly.
5. If evidence is insufficient, say so.
6. Include source URLs for claims supported by web research.
7. Answer the user's actual question directly.
8. Do not describe internal retrieval mechanics unless asked.
""".strip()


def _get_g4f_client():
    try:
        import g4f
        from g4f.client import Client

        provider = getattr(g4f.Provider, "AnyProvider", None)
        if provider is None:
            try:
                from g4f.providers.any_provider import AnyProvider
                provider = AnyProvider
            except Exception:
                try:
                    from g4f.Provider.any_provider import AnyProvider
                    provider = AnyProvider
                except Exception:
                    provider = None

        return Client(), provider
    except Exception as e:
        raise RuntimeError(
            "g4f is required for terminal answers. Install it with: pip install -U g4f"
        ) from e


def _extract_g4f_text(response):
    try:
        choices = getattr(response, "choices", None)
        if choices:
            message = getattr(choices[0], "message", None)
            content = getattr(message, "content", None)
            if content:
                return str(content).strip()
    except Exception:
        pass
    return ""


def _answer_with_llm(client, provider, question, history, evidence_records):
    evidence_blocks = []
    for i, record in enumerate(evidence_records[:MAX_SEARCH_RESULTS], 1):
        evidence = (
            record.get("evidence")
            or record.get("content", "")[:5000]
            or record.get("snippet", "")
        )
        evidence_blocks.append(
            f"SOURCE {i}\n"
            f"Title: {record.get('title', '')}\n"
            f"URL: {record.get('url', '')}\n"
            f"Fresh: {not record.get('cache', False)}\n"
            f"Evidence:\n{evidence}\n"
        )

    evidence_context = "\n".join(evidence_blocks)
    messages = [{"role": "system", "content": CLI_SYSTEM_PROMPT}]
    messages.extend(history[-8:])

    if evidence_context:
        messages.append({
            "role": "system",
            "content": "WEB EVIDENCE FOR THIS QUESTION:\n\n" + evidence_context,
        })

    messages.append({"role": "user", "content": question})

    kwargs = {
        "model": os.getenv("NIZAMI_MODEL", "gpt-4"),
        "messages": messages,
        "stream": False,
    }
    if provider is not None:
        kwargs["provider"] = provider

    try:
        response = client.chat.completions.create(**kwargs)
    except Exception as first_error:
        # Some g4f releases reject an explicit provider.
        if provider is not None:
            kwargs.pop("provider", None)
            try:
                response = client.chat.completions.create(**kwargs)
            except Exception as second_error:
                raise RuntimeError(
                    f"LLM request failed: {type(second_error).__name__}: {second_error}"
                ) from second_error
        else:
            raise RuntimeError(
                f"LLM request failed: {type(first_error).__name__}: {first_error}"
            ) from first_error

    answer = _extract_g4f_text(response)
    if not answer:
        raise RuntimeError("The model returned an empty response.")
    return answer


def _print_sources(records):
    if not records:
        print("\nNo sources available for the last question.")
        return

    print("\nSources:")
    for i, r in enumerate(records, 1):
        freshness = "fresh" if not r.get("cache") else "cache"
        title = r.get("title") or r.get("url") or "Untitled"
        print(f"  [{i}] {title} ({freshness})")
        if r.get("url"):
            print(f"      {r['url']}")


def _print_research(records):
    if not records:
        print("\nNo research evidence available.")
        return

    print("\nResearch evidence:")
    for i, r in enumerate(records, 1):
        print(f"\n[{i}] {r.get('title', '')}")
        print(f"URL: {r.get('url', '')}")
        print(f"Score: {r.get('score', r.get('relevance_score', 'n/a'))}")
        evidence = r.get("evidence") or r.get("content", "")
        print(textwrap.fill(evidence[:3000], width=100))


def _run_research(question):
    global LAST_RESEARCH, LAST_QUERY

    LAST_QUERY = question
    analysis = local_query_analysis(question)

    print("\n[research] searching Bing + DuckDuckGo...", flush=True)
    result = web_search(
        question,
        required_entities=analysis["entities"],
        required_concepts=analysis["concepts"],
        max_results=MAX_SEARCH_RESULTS,
    )

    fresh = []
    for item in result.get("results", []):
        item["cache"] = False

        if len(fresh) < MAX_FETCH_RESULTS and item.get("url"):
            page = fetch_page_tool(
                item["url"],
                question=question,
                required_entities=analysis["entities"],
                required_concepts=analysis["concepts"],
                index_in_cache=True,
            )
            if page.get("success"):
                item["content"] = page.get("content", "")
                item["evidence"] = page.get("evidence", "")
                item["title"] = page.get("title") or item.get("title", "")

        fresh.append(item)

    # Cache is supplementary; fresh sources always stay first.
    try:
        cached = search_cache(
            question,
            required_entities=analysis["entities"],
            required_concepts=analysis["concepts"],
            max_results=4,
        ).get("results", [])
    except Exception:
        cached = []

    combined = fresh + cached
    seen = set()
    deduped = []
    for item in combined:
        key = canonical_url(item.get("url", "")) or item.get("url", "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    LAST_RESEARCH = deduped[:MAX_SEARCH_RESULTS]
    print(f"[research] {len(LAST_RESEARCH)} usable sources", flush=True)
    return LAST_RESEARCH


def _show_help():
    print("""
Commands:
  /help              Show this help
  /exit, /quit, /q   Exit
  /clear, /new       Clear conversation
  /history           Show recent conversation
  /sources           Show sources for the last answer
  /research          Show evidence from the last search
  /search QUERY      Run web research for QUERY
  /open URL          Fetch and display a webpage
""".strip())


def run_cli():
    requests.packages.urllib3.disable_warnings()

    print("=" * 72)
    print("NIZAMI WEB RESEARCH CLI".center(72))
    print("=" * 72)
    print("Ask a question. Fresh web research runs automatically.")
    print("Type /help for commands or /exit to quit.\n")

    try:
        client, provider = _get_g4f_client()
    except Exception as e:
        print(f"Startup error: {e}")
        return 1

    history = []

    # OpenSearch is optional. The CLI still works if it is unavailable.
    if ensure_opensearch_index():
        print(f"OpenSearch: ONLINE ({OPENSEARCH_URL})")
    else:
        print("OpenSearch: offline/unavailable — continuing without cache.")

    while True:
        try:
            user_input = input("\nYou > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            break

        if not user_input:
            continue

        lower = user_input.lower()

        if lower in {"/exit", "/quit", "/q"}:
            print("Exiting...")
            break

        if lower in {"/clear", "/new"}:
            history.clear()
            LAST_RESEARCH.clear()
            global LAST_QUERY
            LAST_QUERY = ""
            print("Conversation cleared.")
            continue

        if lower == "/help":
            _show_help()
            continue

        if lower == "/sources":
            _print_sources(LAST_RESEARCH)
            continue

        if lower == "/research":
            _print_research(LAST_RESEARCH)
            continue

        if lower == "/history":
            if not history:
                print("\nNo conversation history.")
            else:
                print("\nHistory:")
                for m in history[-10:]:
                    role = "You" if m["role"] == "user" else "Nizami"
                    print(f"{role}: {m['content']}")
            continue

        if lower.startswith("/open "):
            url = user_input[6:].strip()
            page = fetch_page_tool(url, index_in_cache=True)
            if not page.get("success"):
                print(f"\nFailed: {page.get('error', 'unknown error')}")
            else:
                print(f"\n{page.get('title', '')}")
                print(page.get("url", url))
                print("\n" + page.get("content", "")[:12000])
            continue

        if lower.startswith("/search "):
            question = user_input[8:].strip()
            if not question:
                print("Usage: /search QUERY")
                continue
        else:
            question = user_input

        try:
            records = _run_research(question)
            answer = _answer_with_llm(
                client,
                provider,
                question,
                history,
                records,
            )
            print(f"\nNizami > {answer}")

            history.append({"role": "user", "content": question})
            history.append({"role": "assistant", "content": answer})

        except Exception as e:
            print(f"\nError: {type(e).__name__}: {e}")
            print("Tip: try the question again or use /search <query>.")

    return 0


if __name__ == "__main__":
    raise SystemExit(run_cli())
