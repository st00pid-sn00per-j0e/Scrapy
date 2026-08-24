#!/usr/bin/env python3
"""
Nizami Web Research MCP Server — Relevance-Hardened v3
======================================================
A full MCP (Model Context Protocol) server that crawls publicly available
sites using Scrapling for ultra-fast fetching and parsing.

Tools exposed:
  • analyze_query      – deterministic intent/entity/concept extraction
  • search_web         – Bing + DuckDuckGo with hard relevance ranking
  • fetch_page         – single-page fetch via Scrapling (static or JS-rendered)
  • extract_evidence   – sentence-level relevance scoring from page text
  • research_question  – end-to-end pipeline: plan → search → fetch → evidence
  • crawl_domain       – recursive domain crawler with link extraction

Install dependencies:
  pip install fastmcp scrapling requests beautifulsoup4
  scrapling install          # one-time browser setup (only if using JS fetcher)

Run the server:
  python nizami_mcp_server.py

Or with stdio transport (default for MCP clients):
  python nizami_mcp_server.py
"""

from __future__ import annotations

import os
import re
import time
import html as html_module
import json
import base64
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------
# MCP
# ------------------------------------------------------------------
from fastmcp import FastMCP

mcp = FastMCP("nizami-web-research")

# ------------------------------------------------------------------
# Scrapling — fast fetcher + adaptive parser
# ------------------------------------------------------------------
try:
    from scrapling.fetchers import Fetcher, StealthyFetcher
    from scrapling import Response as ScraplingResponse
    SCRAPLING_OK = True
except ImportError:
    SCRAPLING_OK = False
    Fetcher = StealthyFetcher = None

# ------------------------------------------------------------------
# Optional g4f for AI query planning (client can also plan manually)
# ------------------------------------------------------------------
try:
    import g4f
    from g4f.client import Client as G4FClient
    G4F_OK = True
except ImportError:
    G4F_OK = False
    G4FClient = None

# ============================================================
# CONFIGURATION
# ============================================================

WIDTH = 100
REQUEST_TIMEOUT = 20
MAX_SEARCH_RESULTS = 8
MAX_FETCH_RESULTS = 6
MAX_PAGE_CHARS = 14000
MAX_EVIDENCE_CHARS = 8000
MAX_LLM_CONTEXT = 14000
MAX_PLANNED_SEARCH_QUERIES = 5
MAX_TOTAL_SEARCH_CANDIDATES = 60
MAX_CRAWL_PAGES = 20
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
    "search", "look", "up", "know", "information", "info"
}

GENERIC_CONCEPTS = {
    "current", "currently", "today", "latest", "recent", "online", "web",
    "information", "info", "about", "details", "person", "company"
}

ROLE_TERMS = {
    "ceo": "CEO",
    "chief executive officer": "CEO",
    "founder": "founder",
    "co-founder": "co-founder",
    "owner": "owner",
    "director": "director",
    "president": "president",
    "chairman": "chairman",
    "chairperson": "chairperson",
    "managing director": "managing director",
    "executive": "executive",
    "cto": "CTO",
    "cfo": "CFO",
    "coo": "COO",
    "head": "head",
    "leader": "leader",
    "leadership": "leadership",
}

QUESTION_INTENT_TERMS = [
    ("how much", "quantity"),
    ("how many", "quantity"),
    ("who is", "identity"),
    ("who", "identity"),
    ("what is", "fact"),
    ("what", "fact"),
    ("when", "date"),
    ("where", "location"),
    ("price", "price"),
    ("cost", "price"),
    ("latest", "current_events"),
    ("recent", "current_events"),
    ("news", "current_events"),
    ("compare", "comparison"),
    ("difference", "comparison"),
    ("review", "review"),
]

TRUSTED_DOMAIN_SCORES = {
    "microsoft.com": 100,
    "apple.com": 100,
    "google.com": 100,
    "amazon.com": 95,
    "meta.com": 95,
    "openai.com": 100,
    "ibm.com": 95,
    "intel.com": 95,
    "nvidia.com": 95,
    "oracle.com": 95,
    "linkedin.com": 72,
    "reuters.com": 72,
    "apnews.com": 70,
    "bbc.com": 68,
    "bbc.co.uk": 68,
    "bloomberg.com": 68,
    "nytimes.com": 65,
    "wsj.com": 65,
    "forbes.com": 58,
    "cnbc.com": 58,
    "theguardian.com": 55,
    "wikipedia.org": 35,
}

LOW_AUTHORITY_DOMAINS = {
    "electricalvolt.com",
    "current.com",
    "example.com",
}

KNOWN_COMPANIES = {
    "microsoft": "microsoft.com",
    "apple": "apple.com",
    "google": "google.com",
    "amazon": "amazon.com",
    "meta": "meta.com",
    "openai": "openai.com",
    "nvidia": "nvidia.com",
    "ibm": "ibm.com",
    "intel": "intel.com",
    "oracle": "oracle.com",
    "samsung": "samsung.com",
    "tesla": "tesla.com",
    "hyundai": "hyundai.com",
    "toyota": "toyota.com",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# ============================================================
# TEXT / URL HELPERS
# ============================================================

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = html_module.unescape(str(text))
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+(?:[._'-][a-z0-9]+)*", text.lower())


def meaningful_tokens(text: str) -> set:
    return {
        t for t in tokenize(text)
        if len(t) >= 3 and t not in STOPWORDS
    }


def exact_word_present(term: str, text: str) -> bool:
    term = clean_text(term).lower()
    text = clean_text(text).lower()
    if not term:
        return False
    return bool(
        re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", text)
    )


def phrase_tokens(term: str) -> List[str]:
    return [
        t for t in tokenize(term)
        if len(t) >= 2 and t not in STOPWORDS
    ]


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = html_module.unescape(url).strip()

    if "bing.com/ck/a" in url:
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            encoded = params.get("u", [])
            if encoded:
                value = encoded[0]
                if value.startswith("a1"):
                    decoded = base64.b64decode(
                        value[2:] + "=="
                    ).decode("utf-8", errors="ignore")
                    if decoded.startswith(("http://", "https://")):
                        url = decoded
        except Exception:
            pass

    try:
        parsed = urlparse(url)._replace(fragment="")
        return parsed.geturl()
    except Exception:
        return url


def canonical_url(url: str) -> str:
    url = normalize_url(url)
    if not url:
        return ""
    try:
        p = urlparse(url)
        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = p.path.rstrip("/") or "/"
        ignored = {"utm_source", "utm_medium", "utm_campaign",
                    "utm_term", "utm_content", "gclid", "fbclid"}
        parts = []
        for key, values in parse_qs(p.query, keep_blank_values=True).items():
            if key.lower() in ignored:
                continue
            for value in values:
                parts.append(f"{key}={value}")
        query = "&".join(sorted(parts))
        return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else "")
    except Exception:
        return url


def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def dedupe_strings(items: List[str]) -> List[str]:
    out = []
    seen = set()
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
# QUERY NORMALIZATION + ANALYSIS
# ============================================================

def normalize_query(user_input: str) -> str:
    query = clean_text(user_input)
    prefixes = [
        r"^/search\s+",
        r"^search\s+the\s+web\s+for\s+",
        r"^search\s+the\s+web\s+",
        r"^web\s+search\s+for\s+",
        r"^web\s+search\s+",
        r"^search\s+online\s+for\s+",
        r"^search\s+online\s+",
        r"^google\s+this\s+",
        r"^look\s+this\s+up\s+",
        r"^look\s+it\s+up\s+",
        r"^do\s+a\s+web\s+search\s+and\s+find\s+",
        r"^do\s+a\s+web\s+search\s+",
        r"^find\s+",
    ]
    for pattern in prefixes:
        new_query = re.sub(pattern, "", query, flags=re.IGNORECASE)
        if new_query != query:
            query = new_query
            break
    return clean_text(query.strip(" ?!.")).strip()


def detect_intent(question: str) -> str:
    lower = question.lower()
    for term, intent in QUESTION_INTENT_TERMS:
        if re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", lower):
            return intent
    return "general_fact"


def extract_capitalized_entities(question: str) -> List[str]:
    entities = []
    pattern = r"\b[A-Z][A-Za-z0-9&.-]*(?:\s+[A-Z][A-Za-z0-9&.-]*){0,4}"
    for match in re.findall(pattern, question):
        candidate = clean_text(match).strip(" ,.!?:")
        if not candidate:
            continue
        if candidate.lower() in {"who", "what", "when", "where", "why", "how",
                                  "tell", "find", "search", "please"}:
            continue
        if len(tokenize(candidate)) == 1 and any(
            candidate.lower() == r.lower() for r in ROLE_TERMS
        ):
            continue
        if len(candidate) >= 3:
            entities.append(candidate)
    return dedupe_strings(entities)


def local_query_analysis(question: str) -> Dict[str, Any]:
    q = normalize_query(question)
    entities = []
    concepts = []

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
    concepts = [
        c for c in dedupe_strings(concepts)
        if c.lower() not in GENERIC_CONCEPTS
    ]

    return {
        "intent": detect_intent(q),
        "entities": entities[:8],
        "concepts": concepts[:8],
    }


# ============================================================
# SEARCH QUERY VALIDATION
# ============================================================

def query_contains_entity(query: str, entity: str) -> bool:
    if exact_word_present(entity, query):
        return True
    aliases = {
        "microsoft": {"microsoft", "msft"},
        "apple": {"apple", "apple inc"},
        "google": {"google", "alphabet"},
        "meta": {"meta", "facebook"},
        "openai": {"openai"},
        "amazon": {"amazon", "amazon.com"},
        "nvidia": {"nvidia"},
        "ibm": {"ibm"},
        "intel": {"intel"},
        "oracle": {"oracle"},
    }
    for alias in aliases.get(entity.lower(), {entity.lower()}):
        if exact_word_present(alias, query):
            return True
    return False


def query_contains_concept(query: str, concept: str) -> bool:
    if exact_word_present(concept, query):
        return True
    tokens = phrase_tokens(concept)
    if len(tokens) >= 2:
        q_tokens = set(tokenize(query))
        matched = sum(1 for token in tokens if token in q_tokens)
        return matched >= max(1, len(tokens) // 2)
    return False


def validate_search_query(
    candidate: str, required_entities: List[str], required_concepts: List[str]
) -> bool:
    candidate = clean_text(candidate)
    if len(candidate) < 3:
        return False
    for entity in required_entities:
        if not query_contains_entity(candidate, entity):
            return False
    if required_concepts:
        if not any(
            query_contains_concept(candidate, concept)
            for concept in required_concepts
        ):
            return False
    return True


def get_domain_for_entity(entity: str) -> str:
    return KNOWN_COMPANIES.get(entity.lower(), "")


def generate_deterministic_queries(
    question: str, entities: List[str], concepts: List[str], intent: str
) -> List[str]:
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
        queries.append(f"{q} 2026")
    validated = []
    for candidate in queries:
        if validate_search_query(candidate, entities, concepts):
            validated.append(candidate)
    if validate_search_query(q, entities, concepts):
        validated.append(q)
    return dedupe_strings(validated)[:MAX_PLANNED_SEARCH_QUERIES]


# ============================================================
# AI QUERY PLANNING (optional — requires g4f)
# ============================================================

def _extract_response_content(response) -> str:
    try:
        choices = getattr(response, "choices", None)
        if not choices:
            return ""
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None)
        return content.strip() if content else ""
    except Exception:
        return ""


def plan_search_queries(client, question: str) -> Dict[str, Any]:
    q = normalize_query(question)
    local = local_query_analysis(q)

    if not G4F_OK or client is None:
        local_queries = generate_deterministic_queries(
            q, local["entities"], local["concepts"], local["intent"]
        )
        return {
            "intent": local["intent"],
            "entities": local["entities"],
            "concepts": local["concepts"],
            "queries": local_queries,
        }

    prompt = f"""You are ONLY a web-search query planner.
Return ONLY valid JSON:
{{
  "intent": "short intent",
  "entities": ["specific named entities"],
  "concepts": ["specific requested attributes"],
  "queries": ["2 to {MAX_PLANNED_SEARCH_QUERIES} search queries"]
}}
Rules:
1. Preserve every important named entity.
2. Preserve the requested attribute.
3. Never replace a company/person/product with a generic term.
4. Queries must be independently useful to Bing or DuckDuckGo.
5. For executive questions, every query must contain the company and requested role.
6. Roles such as CEO, founder, director, and leadership are concepts, NEVER named entities.
7. Do not use generic words such as "current", "about", "person", "information" as the only relevance signal.
8. Do not answer the question.

USER QUESTION:
{q}

LOCAL ANALYSIS:
{json.dumps(local, ensure_ascii=False)}
""".strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            provider=getattr(g4f.Provider, "AnyProvider", None),
            messages=[
                {"role": "system", "content": "Return valid JSON only. Plan retrieval; do not answer."},
                {"role": "user", "content": prompt},
            ],
            stream=False,
        )
        raw = _extract_response_content(response)
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.IGNORECASE)
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Planner did not return an object.")

        intent = clean_text(data.get("intent", "")) or local["intent"]
        ai_entities = [clean_text(x) for x in data.get("entities", []) if isinstance(x, str)]
        ai_concepts = [clean_text(x) for x in data.get("concepts", []) if isinstance(x, str)]
        ai_queries = [clean_text(x)[:300] for x in data.get("queries", []) if isinstance(x, str)]

        role_names = {r.lower() for r in ROLE_TERMS}
        safe_ai_entities = [
            x for x in ai_entities
            if x.lower() not in role_names and x.lower() not in GENERIC_CONCEPTS
        ]
        entities = dedupe_strings(local["entities"] + safe_ai_entities)[:10]
        concepts = dedupe_strings(local["concepts"])[:8]

        valid = []
        for candidate in ai_queries:
            if validate_search_query(candidate, entities, concepts):
                valid.append(candidate)
        valid.extend(generate_deterministic_queries(q, entities, concepts, intent))
        valid = dedupe_strings(valid)[:MAX_PLANNED_SEARCH_QUERIES]

        if valid:
            return {
                "intent": intent,
                "entities": entities,
                "concepts": concepts,
                "queries": valid,
            }
    except Exception:
        pass

    local_queries = generate_deterministic_queries(
        q, local["entities"], local["concepts"], local["intent"]
    )
    return {
        "intent": local["intent"],
        "entities": local["entities"],
        "concepts": local["concepts"],
        "queries": local_queries,
    }


# ============================================================
# BING / DUCKDUCKGO
# ============================================================

def bing_search(query: str, max_results: int = 8) -> List[Dict[str, Any]]:
    try:
        r = SESSION.get(
            "https://www.bing.com/search",
            params={"q": query, "count": max_results},
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
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
        title = clean_text(link.get_text(" ", strip=True))
        snippet_el = item.select_one(".b_caption p")
        snippet = clean_text(snippet_el.get_text(" ", strip=True)) if snippet_el else ""
        if not href:
            continue
        results.append({
            "title": title,
            "url": href,
            "snippet": snippet,
            "engine": "Bing",
            "engine_rank": len(results) + 1,
            "search_query": query,
        })
        if len(results) >= max_results:
            break
    return results


def duckduckgo_search(query: str, max_results: int = 8) -> List[Dict[str, Any]]:
    try:
        r = SESSION.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
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
        title = clean_text(link.get_text(" ", strip=True))
        snippet_el = item.select_one(".result__snippet")
        snippet = clean_text(snippet_el.get_text(" ", strip=True)) if snippet_el else ""
        if not href:
            continue
        results.append({
            "title": title,
            "url": href,
            "snippet": snippet,
            "engine": "DuckDuckGo",
            "engine_rank": len(results) + 1,
            "search_query": query,
        })
        if len(results) >= max_results:
            break
    return results


# ============================================================
# RELEVANCE ENGINE
# ============================================================

def text_for_result(result: Dict[str, Any]) -> str:
    return " ".join([
        result.get("title", ""),
        result.get("snippet", ""),
        result.get("content", ""),
        result.get("evidence", ""),
        result.get("url", ""),
    ])


def query_coverage(query: str, result: Dict[str, Any]) -> float:
    q_tokens = meaningful_tokens(query)
    if not q_tokens:
        return 0.0
    matched = 0
    all_tokens = (
        set(tokenize(result.get("title", "")))
        | set(tokenize(result.get("snippet", "")))
        | set(tokenize(result.get("content", "")))
        | set(tokenize(result.get("url", "")))
    )
    for token in q_tokens:
        if token in all_tokens:
            matched += 1
    return matched / len(q_tokens)


def entity_coverage(result: Dict[str, Any], entities: List[str]) -> float:
    if not entities:
        return 1.0
    text = text_for_result(result)
    matched = sum(
        1 for entity in entities
        if exact_word_present(entity, text) or query_contains_entity(text, entity)
    )
    return matched / len(entities)


def concept_matches_text(concept: str, text: str) -> bool:
    if exact_word_present(concept, text):
        return True
    aliases = {
        "ceo": {"ceo", "chief executive officer", "chief executive"},
        "founder": {"founder", "co-founder", "cofounder", "founded by"},
        "director": {"director", "managing director"},
        "president": {"president"},
        "chairman": {"chairman", "chairperson"},
        "leadership": {"leadership", "management", "executive team", "leadership team"},
        "executive": {"executive", "executive leadership"},
    }
    return any(
        exact_word_present(alias, text)
        for alias in aliases.get(concept.lower(), {concept.lower()})
    )


def concept_coverage(result: Dict[str, Any], concepts: List[str]) -> float:
    if not concepts:
        return 1.0
    text = text_for_result(result)
    matched = sum(
        1 for concept in concepts
        if concept_matches_text(concept, text)
    )
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


def best_query_coverage(result: Dict[str, Any], queries: List[str]):
    if not queries:
        return 0.0, ""
    scored = [(query_coverage(q, result), q) for q in queries]
    return max(scored, key=lambda x: x[0])


def hard_relevance_gate(result: Dict[str, Any], analysis: Dict[str, Any]) -> tuple:
    entities = analysis.get("entities", [])
    concepts = analysis.get("concepts", [])
    queries = analysis.get("queries", [])
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


def rank_results(
    results: List[Dict[str, Any]], question: str, analysis: Dict[str, Any]
) -> List[Dict[str, Any]]:
    ranked = []
    entities = analysis.get("entities", [])
    concepts = analysis.get("concepts", [])
    queries = analysis.get("queries", []) or [question]

    for result in results:
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        url = result.get("url", "")
        domain = get_domain(url)

        allowed, reason = hard_relevance_gate(result, analysis)
        if not allowed:
            result["rejected_reason"] = reason
            continue

        ecover = entity_coverage(result, entities)
        ccover = concept_coverage(result, concepts)
        best_coverage, best_query = best_query_coverage(result, queries)

        title_lower = title.lower()
        snippet_lower = snippet.lower()
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
        authority = domain_authority(domain)
        score += min(authority, 100) * 0.25

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
        if any(part in lower_url for part in (
            "/leadership", "/about", "/company", "/management",
            "/executive", "/people", "/team",
        )):
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
    return ranked[:MAX_SEARCH_RESULTS]


# ============================================================
# WEB SEARCH PIPELINE
# ============================================================

def web_search(question: str, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    start = time.perf_counter()
    all_results = []
    seen_urls = set()
    queries = analysis.get("queries", [])

    for qi, search_query in enumerate(queries, 1):
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
                result["original_query"] = question
                all_results.append(result)
                if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
                    break
            if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
                break
        if len(all_results) >= MAX_TOTAL_SEARCH_CANDIDATES:
            break

    ranked = rank_results(all_results, question, analysis)
    elapsed = time.perf_counter() - start
    return ranked


# ============================================================
# SCRAPLING FETCH + PARSE
# ============================================================

def fetch_page_scrapling(
    url: str,
    render_js: bool = False,
    max_chars: int = MAX_PAGE_CHARS,
    timeout: int = REQUEST_TIMEOUT,
) -> Dict[str, Any]:
    """
    Fetch a page using Scrapling.
    render_js=False → Fetcher (fast static HTTP)
    render_js=True  → StealthyFetcher (headless browser, bypasses anti-bot)
    """
    start = time.perf_counter()
    url = normalize_url(url)
    if not url:
        return {"success": False, "url": "", "error": "Invalid URL", "content": "", "elapsed": 0}

    if not SCRAPLING_OK:
        return {
            "success": False,
            "url": url,
            "error": "Scrapling not installed. Run: pip install scrapling && scrapling install",
            "content": "",
            "elapsed": 0,
        }

    try:
        if render_js:
            page = StealthyFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                timeout=timeout * 1000,  # ms
            )
        else:
            page = Fetcher.fetch(
                url,
                timeout=timeout,
            )

        # Scrapling page objects expose .text, .css(), .xpath(), etc.
        title = ""
        try:
            title_tag = page.css("title", first_match=True)
            if title_tag:
                title = clean_text(title_tag.text)
        except Exception:
            pass

        metadata = []
        try:
            for meta in page.css("meta"):
                key = (meta.attributes.get("name", "") or meta.attributes.get("property", "")).lower()
                content = meta.attributes.get("content", "")
                if key in {"description", "og:description", "author", "og:title"} and content:
                    metadata.append(clean_text(content))
        except Exception:
            pass

        # Fast text extraction via Scrapling's optimized parser
        try:
            body_text = clean_text(page.text)
        except Exception:
            body_text = ""

        content = clean_text(" ".join(metadata) + " " + body_text)[:max_chars]

        # Also keep raw HTML snippet for advanced parsing
        raw_html = ""
        try:
            raw_html = str(page.html)[:20000]
        except Exception:
            pass

        return {
            "success": True,
            "url": url,
            "title": title,
            "content": content,
            "raw_html": raw_html,
            "elapsed": round(time.perf_counter() - start, 2),
            "render_js": render_js,
        }

    except Exception as e:
        return {
            "success": False,
            "url": url,
            "error": f"{type(e).__name__}: {e}",
            "content": "",
            "elapsed": round(time.perf_counter() - start, 2),
        }


# ============================================================
# EVIDENCE EXTRACTION
# ============================================================

def extract_evidence_from_text(
    content: str,
    question: str,
    entities: List[str],
    concepts: List[str],
    max_chars: int = MAX_EVIDENCE_CHARS,
) -> str:
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
    evidence = []
    seen = set()
    total = 0
    for score, sentence in scored:
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        evidence.append(sentence)
        total += len(sentence)
        if total >= max_chars:
            break

    return clean_text(" ".join(evidence))


# ============================================================
# DOMAIN CRAWLER
# ============================================================

def crawl_domain(
    start_url: str,
    max_pages: int = MAX_CRAWL_PAGES,
    same_domain: bool = True,
    render_js: bool = False,
    max_chars: int = MAX_PAGE_CHARS,
) -> List[Dict[str, Any]]:
    """
    Simple breadth-first crawler using Scrapling.
    Extracts internal links and fetches pages up to max_pages.
    """
    if not SCRAPLING_OK:
        return [{
            "success": False,
            "url": start_url,
            "error": "Scrapling not installed. Run: pip install scrapling && scrapling install",
        }]

    start_url = normalize_url(start_url)
    if not start_url:
        return []

    start_domain = get_domain(start_url)
    visited = set()
    queue = [start_url]
    pages = []

    while queue and len(pages) < max_pages:
        url = queue.pop(0)
        key = canonical_url(url)
        if key in visited:
            continue
        visited.add(key)

        page = fetch_page_scrapling(url, render_js=render_js, max_chars=max_chars)
        if not page.get("success"):
            pages.append(page)
            continue

        pages.append(page)

        # Extract links for further crawling
        try:
            if render_js:
                fetched = StealthyFetcher.fetch(url, headless=True, network_idle=True)
            else:
                fetched = Fetcher.fetch(url)

            for link in fetched.css("a[href]"):
                href = link.attributes.get("href", "")
                if not href:
                    continue
                abs_url = urljoin(url, href)
                abs_url = normalize_url(abs_url)
                if not abs_url:
                    continue
                # Skip non-HTTP, anchors, media
                if not abs_url.startswith(("http://", "https://")):
                    continue
                if same_domain and get_domain(abs_url) != start_domain:
                    continue
                canon = canonical_url(abs_url)
                if canon not in visited and canon not in {canonical_url(u) for u in queue}:
                    queue.append(abs_url)
        except Exception:
            pass

    return pages


# ============================================================
# MCP TOOLS
# ============================================================

@mcp.tool()
def analyze_query(question: str) -> Dict[str, Any]:
    """
    Analyze a user question and return intent, named entities, concepts,
    and deterministic search queries.
    """
    analysis = local_query_analysis(question)
    queries = generate_deterministic_queries(
        question,
        analysis["entities"],
        analysis["concepts"],
        analysis["intent"],
    )
    analysis["queries"] = queries
    return analysis


@mcp.tool()
def search_web(
    question: str,
    max_results: int = MAX_SEARCH_RESULTS,
    use_ai_planner: bool = False,
) -> Dict[str, Any]:
    """
    Search the web using Bing + DuckDuckGo, rank results with hard relevance gates.
    Returns validated search results with scores, coverage metrics, and provenance.
    """
    client = G4FClient() if (use_ai_planner and G4F_OK) else None
    analysis = plan_search_queries(client, question)
    results = web_search(question, analysis)
    return {
        "question": question,
        "analysis": {k: v for k, v in analysis.items() if k != "queries"},
        "queries_used": analysis.get("queries", []),
        "result_count": len(results),
        "results": results,
    }


@mcp.tool()
def fetch_page(
    url: str,
    render_js: bool = False,
    max_chars: int = MAX_PAGE_CHARS,
) -> Dict[str, Any]:
    """
    Fetch a single public webpage using Scrapling.
    render_js=True uses a stealth headless browser (slower, but beats anti-bot).
    render_js=False uses fast static HTTP fetching.
    Returns title, extracted text content, raw HTML snippet, and timing.
    """
    return fetch_page_scrapling(url, render_js=render_js, max_chars=max_chars)


@mcp.tool()
def extract_evidence(
    page_content: str,
    question: str,
    entities: List[str] = None,
    concepts: List[str] = None,
    max_chars: int = MAX_EVIDENCE_CHARS,
) -> Dict[str, Any]:
    """
    Extract relevant evidence sentences from page content based on the question,
    entities, and concepts. Returns scored, deduplicated evidence text.
    """
    entities = entities or []
    concepts = concepts or []
    evidence = extract_evidence_from_text(
        page_content, question, entities, concepts, max_chars
    )
    return {
        "question": question,
        "entities": entities,
        "concepts": concepts,
        "evidence": evidence,
        "evidence_length": len(evidence),
    }


@mcp.tool()
def research_question(
    question: str,
    max_fetch: int = MAX_FETCH_RESULTS,
    use_ai_planner: bool = False,
    render_js: bool = False,
) -> Dict[str, Any]:
    """
    End-to-end research pipeline:
    1. Plan search queries (AI or deterministic)
    2. Search Bing + DuckDuckGo
    3. Fetch top pages via Scrapling
    4. Extract evidence with relevance scoring
    5. Return structured sources + evidence context
    """
    start = time.perf_counter()
    client = G4FClient() if (use_ai_planner and G4F_OK) else None
    analysis = plan_search_queries(client, question)

    if not analysis.get("queries"):
        return {"success": False, "error": "No usable search queries generated."}

    search_results = web_search(question, analysis)
    if not search_results:
        return {"success": False, "error": "No validated search results found."}

    retrieved = []
    fetch_count = min(max_fetch, len(search_results))

    for i, result in enumerate(search_results[:fetch_count], 1):
        url = normalize_url(result.get("url", ""))
        page = fetch_page_scrapling(url, render_js=render_js)
        if not page.get("success"):
            retrieved.append({
                **result,
                "fetch_success": False,
                "fetch_error": page.get("error"),
            })
            continue

        page_probe = {
            "title": page.get("title", ""),
            "snippet": page.get("content", "")[:7000],
            "content": page.get("content", ""),
            "url": page.get("url", url),
        }
        allowed, reason = hard_relevance_gate(page_probe, analysis)
        if not allowed:
            retrieved.append({
                **result,
                "fetch_success": True,
                "rejected_after_fetch": True,
                "rejected_reason": reason,
                "content": page.get("content", "")[:500],
            })
            continue

        evidence = extract_evidence_from_text(
            page.get("content", ""),
            question,
            analysis.get("entities", []),
            analysis.get("concepts", []),
        )

        record = {
            **result,
            "url": page.get("url", url),
            "title": page.get("title") or result.get("title", ""),
            "domain": get_domain(page.get("url", url)),
            "content": page.get("content", ""),
            "raw_html": page.get("raw_html", ""),
            "evidence": evidence,
            "fetch_success": True,
            "fetch_elapsed": page.get("elapsed", 0),
            "cache": False,
        }
        retrieved.append(record)

    # Final hard gate on merged set
    final = []
    for item in retrieved:
        if not item.get("fetch_success"):
            continue
        if item.get("rejected_after_fetch"):
            continue
        if analysis.get("entities") and entity_coverage(item, analysis["entities"]) < 1.0:
            continue
        if analysis.get("concepts") and concept_coverage(item, analysis["concepts"]) < 1.0:
            continue
        final.append(item)

    final.sort(key=lambda x: -float(x.get("score", 0)))

    # Build evidence context for LLM consumption
    context_blocks = []
    total = 0
    for i, record in enumerate(final, 1):
        evidence = record.get("evidence", "") or record.get("content", "")[:4000]
        block = (
            f"SOURCE {i}\n"
            f"Title: {record.get('title', '')}\n"
            f"URL: {record.get('url', '')}\n"
            f"Domain: {record.get('domain', '')}\n"
            f"Score: {record.get('score', 0)}\n"
            f"Evidence:\n{evidence}"
        )
        if total + len(block) > MAX_LLM_CONTEXT:
            break
        context_blocks.append(block)
        total += len(block)

    elapsed = time.perf_counter() - start
    return {
        "success": True,
        "question": question,
        "analysis": analysis,
        "source_count": len(final),
        "elapsed_seconds": round(elapsed, 2),
        "sources": final,
        "evidence_context": "\n\n".join(context_blocks),
    }


@mcp.tool()
def crawl_domain_tool(
    start_url: str,
    max_pages: int = 10,
    same_domain: bool = True,
    render_js: bool = False,
    max_chars: int = MAX_PAGE_CHARS,
) -> Dict[str, Any]:
    """
    Crawl a public domain starting from start_url.
    Uses Scrapling for fast static fetches (render_js=False) or
    stealth headless browsing (render_js=True).
    Returns fetched pages with title, content, and links discovered.
    """
    pages = crawl_domain(
        start_url,
        max_pages=min(max_pages, MAX_CRAWL_PAGES),
        same_domain=same_domain,
        render_js=render_js,
        max_chars=max_chars,
    )
    return {
        "start_url": start_url,
        "pages_fetched": len(pages),
        "pages": pages,
    }


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    print("=" * WIDTH)
    print("NIZAMI WEB RESEARCH MCP SERVER".center(WIDTH))
    print("=" * WIDTH)
    print(f"\nScrapling available : {'YES' if SCRAPLING_OK else 'NO (pip install scrapling)'}")
    print(f"g4f AI planner      : {'YES' if G4F_OK else 'NO (optional)'}")
    print(f"Transport           : stdio (default MCP)")
    print(f"\nTools exposed:")
    print("  • analyze_query")
    print("  • search_web")
    print("  • fetch_page")
    print("  • extract_evidence")
    print("  • research_question")
    print("  • crawl_domain_tool")
    print("\n" + "-" * WIDTH)
    mcp.run(transport="stdio")
