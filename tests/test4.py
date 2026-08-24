# """
# Job Title + Person Extractor (GLiNER + Position Linking + Confidence)
# ---------------------------------------------------------------------
# - Extracts HTML blocks
# - GLiNER extracts persons and job titles with character positions + confidence
# - Links each person to the nearest title (skip overlaps)
# - Validates entities by confidence (0.55 for persons, 0.45 for titles)
# - No hardcoded word filters, no fallback regex, no O*NET mapping
# - Uses Scrapling PlayWrightFetcher for JavaScript rendering + scrolling
# """

# import re
# from collections import defaultdict
# from bs4 import BeautifulSoup
# from sentence_transformers import SentenceTransformer
# import numpy as np
# import fasttext
# import os
# import requests
# from gliner import GLiNER

# # ============================================================
# # SCRAPLING FETCHER (using PlayWrightFetcher directly)
# # ============================================================

# try:
#     from scrapling.fetchers import PlayWrightFetcher
#     SCRAPLING_AVAILABLE = True
# except ImportError:
#     SCRAPLING_AVAILABLE = False
#     print("⚠️ PlayWrightFetcher not available. Falling back to requests.")

# def fetch_html(url, wait_for_time=3000, scroll_to_bottom=True, scroll_delay=1000):
#     """
#     Fetch HTML using Scrapling's PlayWrightFetcher (or fallback to requests).
#     """
#     import time

#     if SCRAPLING_AVAILABLE:
#         try:
#             fetcher = PlayWrightFetcher()
#             response = fetcher.get(url)
            
#             if response is None:
#                 raise Exception("Scrapling returned empty response")
            
#             print("[DEBUG] Scrapling response received")
            
#             # Wait for initial content
#             time.sleep(wait_for_time / 1000)
            
#             # If we have a Playwright page, use it for scrolling and content
#             if hasattr(response, "page"):
#                 page = response.page
#                 if scroll_to_bottom:
#                     page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
#                     time.sleep(scroll_delay / 1000)
#                 html = page.content()
#             else:
#                 # Fallback: use response.text (if available)
#                 html = response.text if hasattr(response, 'text') else str(response)
            
#             if not html:
#                 raise Exception("Empty HTML received")
            
#             return html
            
#         except Exception as e:
#             print(f"[DEBUG] Scrapling failed: {e}. Falling back to requests.")
#             # Fall through to requests
    
#     # Fallback: plain requests (no JS rendering)
#     print("[DEBUG] Using requests fallback (no JavaScript rendering).")
#     response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
#     if response.status_code != 200:
#         raise Exception(f"Requests fetch failed with status {response.status_code}")
#     return response.text

# # ============================================================
# # LOAD FASTTEXT MODEL (for code removal)
# # ============================================================

# MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# MODEL_PATH = "code_nl_model.bin"

# if not os.path.exists(MODEL_PATH):
#     print("Downloading FastText model...")
#     response = requests.get(MODEL_URL, stream=True)
#     with open(MODEL_PATH, "wb") as f:
#         for chunk in response.iter_content(chunk_size=1024 * 1024):
#             if chunk:
#                 f.write(chunk)
#     print("Model downloaded.")

# print("Loading FastText model...")
# fasttext_model = fasttext.load_model(MODEL_PATH)
# print("FastText model loaded.")

# def is_code_line(line, threshold=0.5):
#     if not line.strip():
#         return False
#     labels, probs = fasttext_model.predict(line.strip())
#     if labels[0] == "__label__code" and probs[0] > threshold:
#         return True
#     return False

# def clean_code_lines(text):
#     lines = text.splitlines()
#     return "\n".join([line for line in lines if not is_code_line(line)])

# # ============================================================
# # LOAD GLINER MODEL
# # ============================================================

# print("Loading GLiNER...")
# gliner_model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")
# print("GLiNER loaded.")

# # ============================================================
# # HTML BLOCK EXTRACTION (min_words=1 to catch short blocks)
# # ============================================================

# def extract_meaningful_blocks(html, min_words=1):
#     soup = BeautifulSoup(html, "html.parser")
#     for tag in soup(["script", "style", "nav", "footer", "header"]):
#         tag.decompose()
#     blocks = []
#     candidates = soup.find_all(["div", "section", "article", "li"])
#     for element in candidates:
#         text = element.get_text(" ", strip=True)
#         words = text.split()
#         if min_words <= len(words) <= 80:
#             blocks.append(text)
#     seen = set()
#     result = []
#     for b in blocks:
#         if b not in seen:
#             seen.add(b)
#             result.append(b)
#     return result

# # ============================================================
# # GLiNER EXTRACTION WITH POSITIONS + CONFIDENCE
# # ============================================================

# def extract_entities_gliner(text, threshold=0.35):
#     labels = ["person", "job title"]
#     entities = gliner_model.predict_entities(text, labels, threshold=threshold)
#     persons = []
#     titles = []
#     for e in entities:
#         item = {
#             "text": e["text"].strip(),
#             "start": e["start"],
#             "end": e["end"],
#             "score": e.get("score", 0.0)
#         }
#         if e["label"] == "person":
#             persons.append(item)
#         elif e["label"] == "job title":
#             titles.append(item)
#     return persons, titles

# # ============================================================
# # VALIDATION BY CONFIDENCE ONLY
# # ============================================================

# def is_valid_person(entity, min_confidence=0.55):
#     text = entity["text"].strip()
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     # Names normally contain at least two tokens
#     parts = text.split()
#     if len(parts) < 2:
#         return False
#     # Reject if entity contains obvious title structure (e.g., "&", "and")
#     if any(c in text.lower().split() for c in ["&", "and"]):
#         return False
#     return True

# def is_valid_title(entity, min_confidence=0.45):
#     text = entity["text"].strip()
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     # Optionally, you could add a minimum length or other checks, but let the model decide.
#     return True

# # ============================================================
# # LINKER – FIND CLOSEST TITLE FOR EACH PERSON (AVOID OVERLAPS)
# # ============================================================

# def link_person_to_title(persons, titles, max_distance=60):
#     relations = []
#     for person in persons:
#         best_title = None
#         best_distance = 9999
#         for title in titles:
#             # skip overlap: title and person overlap in text
#             if (person["start"] >= title["start"] and person["start"] <= title["end"]) or \
#                (title["start"] >= person["start"] and title["start"] <= person["end"]):
#                 continue
#             distance = abs(person["start"] - title["end"])
#             if distance < best_distance:
#                 best_distance = distance
#                 best_title = title
#         if best_title and best_distance <= max_distance:
#             relations.append({
#                 "person": person["text"],
#                 "title": best_title["text"],
#                 "distance": best_distance,
#                 "confidence": (person.get("score", 0) + best_title.get("score", 0)) / 2
#             })
#     return relations

# def remove_duplicate_links(links):
#     seen = set()
#     clean = []
#     for link in links:
#         key = (link["person"], link["title"])
#         if key not in seen:
#             seen.add(key)
#             clean.append(link)
#     return clean

# # ============================================================
# # STRIP HTML (for post‑processing – now unused but kept for clarity)
# # ============================================================

# def strip_html(text):
#     text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
#     text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
#     text = re.sub(r'<[^>]+>', ' ', text)
#     text = re.sub(r'\s+', ' ', text)
#     return text.strip()

# # ============================================================
# # MAIN EXTRACTION FUNCTION (NO FALLBACK, NO POST‑PROCESSING)
# # ============================================================

# def extract_people_near_titles(html, debug=True):
#     blocks = extract_meaningful_blocks(html)
#     print("\n[DEBUG] BLOCK COUNT:", len(blocks))
#     for i, block in enumerate(blocks[:5]):
#         print(f"\n--- BLOCK {i} ---")
#         print(block[:500])

#     results = defaultdict(list)  # title -> list of (person, confidence, distance)

#     total_blocks = len(blocks)
#     total_persons = 0
#     total_titles = 0
#     total_relations = 0

#     for block_idx, block in enumerate(blocks):
#         block_clean = clean_code_lines(block) if len(block.splitlines()) > 5 else block

#         # GLiNER extraction (includes confidence)
#         persons, titles = extract_entities_gliner(block_clean, threshold=0.35)

#         # Filter by confidence / validity
#         persons = [p for p in persons if is_valid_person(p)]
#         titles = [t for t in titles if is_valid_title(t)]

#         total_persons += len(persons)
#         total_titles += len(titles)

#         if debug:
#             print(f"\n[DEBUG] Block {block_idx+1} PERSONS:")
#             if persons:
#                 for p in persons:
#                     print(f"  {p['text']} (start={p['start']}, end={p['end']}, score={p['score']:.2f})")
#             else:
#                 print("  (No persons)")
#             print(f"[DEBUG] Block {block_idx+1} TITLES:")
#             if titles:
#                 for t in titles:
#                     print(f"  {t['text']} (start={t['start']}, end={t['end']}, score={t['score']:.2f})")
#             else:
#                 print("  (No titles)")

#         # Link persons to titles (new linker)
#         links = link_person_to_title(persons, titles, max_distance=60)
#         links = remove_duplicate_links(links)

#         total_relations += len(links)

#         if debug:
#             print(f"[DEBUG] Block {block_idx+1} LINKS:")
#             for link in links:
#                 print(f"  {link['person']} -> {link['title']} (distance={link['distance']}, confidence={link['confidence']:.2f})")

#         # Store results
#         for link in links:
#             results[link["title"]].append((link["person"], link["confidence"], link["distance"]))

#     print("\n[DEBUG] SUMMARY:")
#     print(f"  Total blocks: {total_blocks}")
#     print(f"  Total persons: {total_persons}")
#     print(f"  Total titles: {total_titles}")
#     print(f"  Total person-title links: {total_relations}")

#     # Deduplicate per title: keep the highest confidence person (or best distance)
#     final = {}
#     for title, pairs in results.items():
#         # sort by confidence descending, then by distance ascending
#         sorted_pairs = sorted(pairs, key=lambda x: (-x[1], x[2]))
#         # keep unique persons (in case of duplicates)
#         seen_names = set()
#         unique = []
#         for name, conf, dist in sorted_pairs:
#             if name not in seen_names:
#                 seen_names.add(name)
#                 unique.append({"name": name, "confidence": conf, "distance": dist})
#         final[title] = unique

#     return final

# # ============================================================
# # MAIN
# # ============================================================

# if __name__ == "__main__":
#     print("Job Title + Person Extractor (GLiNER + Confidence + Linker)")
#     print("Paste HTML or a URL (starts with http). Type exit when done.\n")

#     lines = []
#     while True:
#         line = input()
#         if line.strip().lower() == "exit":
#             break
#         lines.append(line)

#     user_input = "\n".join(lines).strip()

#     # Clean up common markdown/typo issues
#     if user_input.startswith("[") and "]" in user_input:
#         user_input = user_input.split("](")[-1].rstrip(")")

#     # Check if it's a URL
#     if user_input.startswith("http://") or user_input.startswith("https://"):
#         print(f"Fetching URL: {user_input}")
#         try:
#             html = fetch_html(
#                 user_input,
#                 wait_for_time=3000,
#                 scroll_to_bottom=True,
#                 scroll_delay=1500
#             )
#             print("Page fetched successfully.")
#         except Exception as e:
#             print(f"Error fetching URL: {e}")
#             html = None
#     else:
#         html = user_input

#     if html:
#         results = extract_people_near_titles(html, debug=True)

#         print("\n=== RESULTS ===\n")
#         if not results:
#             print("No person-role relationships found.")
#         else:
#             for title, persons in results.items():
#                 print(f"Title: {title}")
#                 for entry in persons:
#                     print(f"  - {entry['name']} (confidence: {entry['confidence']:.2f}, distance: {entry['distance']})")
#                 print()
#     else:
#         print("No HTML to process.")











# """
# Job Title + Person Extractor (GLiNER + Confidence + Linker + BFS Crawler)
# ----------------------------------------------------------------------------
# - Crawls entire domain using Scrapling PlayWrightFetcher (BFS, same-domain)
# - Extracts HTML blocks from each page
# - GLiNER extracts persons and job titles with character positions + confidence
# - Links each person to the nearest title (skip overlaps)
# - Validates entities by confidence (0.55 for persons, 0.45 for titles)
# - No hardcoded word filters, no fallback regex, no O*NET mapping
# """

# import re
# from collections import defaultdict, deque
# from bs4 import BeautifulSoup
# from sentence_transformers import SentenceTransformer
# import numpy as np
# import fasttext
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from queue import Queue
# import threading
# import os
# import requests
# from gliner import GLiNER
# from urllib.parse import urljoin, urlparse

# # ============================================================
# # SCRAPLING FETCHER (using PlayWrightFetcher directly)
# # ============================================================

# try:
#     from scrapling.fetchers import StealthyFetcher
#     SCRAPLING_AVAILABLE = True
# except ImportError:
#     SCRAPLING_AVAILABLE = False
#     print("⚠️ PlayWrightFetcher not available. Falling back to requests.")

# def fetch_html(url, wait_for_time=3000, scroll_to_bottom=True, scroll_delay=1000):
#     """
#     Fetch HTML using Scrapling StealthyFetcher (Scrapling 0.4.14)
#     with requests fallback.
#     """

#     import time

#     if SCRAPLING_AVAILABLE:

#         try:

#             print("[DEBUG] Using Scrapling StealthyFetcher")

#             response = StealthyFetcher.fetch(
#                 url,
#                 headless=True,
#                 timeout=30000
#             )


#             if response is None:
#                 raise Exception(
#                     "Scrapling returned empty response"
#                 )


#             print("[DEBUG] Scrapling response received")


#             # Optional delay for JS hydration
#             time.sleep(
#                 wait_for_time / 1000
#             )


#             # Scrapling Response exposes html
#             if hasattr(response, "html"):

#                 html = response.html

#             else:

#                 html = str(response)



#             if not html:

#                 raise Exception(
#                     "Empty HTML received"
#                 )


#             return html



#         except Exception as e:

#             print(
#                 f"[DEBUG] Scrapling failed: {e}"
#             )


#     # ========================================================
#     # REQUESTS FALLBACK
#     # ========================================================

#     print(
#         "[DEBUG] Using requests fallback"
#     )


#     response = requests.get(
#         url,
#         timeout=30,
#         headers={
#             "User-Agent":
#             "Mozilla/5.0"
#         }
#     )


#     if response.status_code != 200:

#         raise Exception(
#             f"Requests failed: {response.status_code}"
#         )


#     return response.text

# # ============================================================
# # FULL DOMAIN BFS CRAWLER USING SCRAPLING
# # ============================================================

# def normalize_url(url):
#     """
#     Remove fragments and normalize URLs.
#     """
#     parsed = urlparse(url)
#     return parsed._replace(fragment="").geturl().rstrip("/")

# def is_same_domain(url, base_domain):
#     """
#     Prevent leaving target website.
#     """
#     parsed = urlparse(url)
#     return (parsed.netloc == base_domain or parsed.netloc.endswith("." + base_domain))

# def extract_links_from_html(html, current_url):
#     """
#     Extract internal links from rendered HTML.
#     """
#     soup = BeautifulSoup(html, "html.parser")
#     links = []
#     for a in soup.find_all("a", href=True):
#         href = a["href"].strip()
#         if (href.startswith("#") or href.startswith("mailto:") or
#             href.startswith("tel:") or href.startswith("javascript:")):
#             continue
#         absolute = urljoin(current_url, href)
#         links.append(normalize_url(absolute))
#     return links

# def crawl_domain_bfs(start_url, max_pages=100, max_depth=3, wait_for_time=3000):
#     """
#     BFS crawler restricted to starting domain.
#     Returns: {url: html}
#     """
#     start_url = normalize_url(start_url)
#     parsed = urlparse(start_url)
#     base_domain = parsed.netloc

#     queue = deque()
#     queue.append((start_url, 0))
#     visited = set()
#     pages = {}

#     print("\n========== BFS CRAWLER START ==========")
#     print("Domain:", base_domain)

#     while queue and len(pages) < max_pages:
#         current_url, depth = queue.popleft()
#         if current_url in visited:
#             continue
#         if depth > max_depth:
#             continue
#         visited.add(current_url)

#         print(f"\n[CRAWL] Depth={depth} {current_url}")

#         try:
#             html = fetch_html(current_url, wait_for_time=wait_for_time,
#                               scroll_to_bottom=True, scroll_delay=1000)
#             pages[current_url] = html
#             print(f"[OK] HTML size: {len(html)}")
#         except Exception as e:
#             print(f"[FAILED] {current_url}")
#             print(e)
#             continue

#         links = extract_links_from_html(html, current_url)
#         print(f"[LINKS FOUND] {len(links)}")

#         for link in links:
#             if link in visited:
#                 continue
#             if not is_same_domain(link, base_domain):
#                 continue
#             queue.append((link, depth + 1))

#     print("\n========== BFS COMPLETE ==========")
#     print("Pages crawled:", len(pages))
#     return pages

# # ============================================================
# # LOAD FASTTEXT MODEL (for code removal)
# # ============================================================

# MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# MODEL_PATH = "code_nl_model.bin"

# if not os.path.exists(MODEL_PATH):
#     print("Downloading FastText model...")
#     response = requests.get(MODEL_URL, stream=True)
#     with open(MODEL_PATH, "wb") as f:
#         for chunk in response.iter_content(chunk_size=1024 * 1024):
#             if chunk:
#                 f.write(chunk)
#     print("Model downloaded.")

# print("Loading FastText model...")
# fasttext_model = fasttext.load_model(MODEL_PATH)
# print("FastText model loaded.")

# def is_code_line(line, threshold=0.5):
#     if not line.strip():
#         return False
#     labels, probs = fasttext_model.predict(line.strip())
#     if labels[0] == "__label__code" and probs[0] > threshold:
#         return True
#     return False

# def clean_code_lines(text):
#     lines = text.splitlines()
#     return "\n".join([line for line in lines if not is_code_line(line)])

# # ============================================================
# # LOAD GLINER MODEL
# # ============================================================

# print("Loading GLiNER...")
# gliner_model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")
# print("GLiNER loaded.")

# # ============================================================
# # HTML BLOCK EXTRACTION (min_words=1 to catch short blocks)
# # ============================================================

# def extract_meaningful_blocks(html, min_words=1):
#     soup = BeautifulSoup(html, "html.parser")
#     for tag in soup(["script", "style", "nav", "footer", "header"]):
#         tag.decompose()
#     blocks = []
#     candidates = soup.find_all(["div", "section", "article", "li"])
#     for element in candidates:
#         text = element.get_text(" ", strip=True)
#         words = text.split()
#         if min_words <= len(words) <= 80:
#             blocks.append(text)
#     seen = set()
#     result = []
#     for b in blocks:
#         if b not in seen:
#             seen.add(b)
#             result.append(b)
#     return result

# # ============================================================
# # GLiNER EXTRACTION WITH POSITIONS + CONFIDENCE
# # ============================================================

# def extract_entities_gliner(text, threshold=0.35):
#     labels = ["person", "job title"]
#     entities = gliner_model.predict_entities(text, labels, threshold=threshold)
#     persons = []
#     titles = []
#     for e in entities:
#         item = {
#             "text": e["text"].strip(),
#             "start": e["start"],
#             "end": e["end"],
#             "score": e.get("score", 0.0)
#         }
#         if e["label"] == "person":
#             persons.append(item)
#         elif e["label"] == "job title":
#             titles.append(item)
#     return persons, titles

# # ============================================================
# # VALIDATION BY CONFIDENCE ONLY
# # ============================================================

# def is_valid_person(entity, min_confidence=0.55):
#     text = entity["text"].strip()
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     parts = text.split()
#     if len(parts) < 2:
#         return False
#     if any(c in text.lower().split() for c in ["&", "and"]):
#         return False
#     return True

# def is_valid_title(entity, min_confidence=0.45):
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     return True

# # ============================================================
# # LINKER – FIND CLOSEST TITLE FOR EACH PERSON (AVOID OVERLAPS)
# # ============================================================

# def link_person_to_title(persons, titles, max_distance=60):
#     relations = []
#     for person in persons:
#         best_title = None
#         best_distance = 9999
#         for title in titles:
#             # skip overlap: title and person overlap in text
#             if (person["start"] >= title["start"] and person["start"] <= title["end"]) or \
#                (title["start"] >= person["start"] and title["start"] <= person["end"]):
#                 continue
#             distance = abs(person["start"] - title["end"])
#             if distance < best_distance:
#                 best_distance = distance
#                 best_title = title
#         if best_title and best_distance <= max_distance:
#             relations.append({
#                 "person": person["text"],
#                 "title": best_title["text"],
#                 "distance": best_distance,
#                 "confidence": (person.get("score", 0) + best_title.get("score", 0)) / 2
#             })
#     return relations

# def remove_duplicate_links(links):
#     seen = set()
#     clean = []
#     for link in links:
#         key = (link["person"], link["title"])
#         if key not in seen:
#             seen.add(key)
#             clean.append(link)
#     return clean

# # ============================================================
# # STRIP HTML (for post‑processing – now unused but kept for clarity)
# # ============================================================

# def strip_html(text):
#     text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
#     text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
#     text = re.sub(r'<[^>]+>', ' ', text)
#     text = re.sub(r'\s+', ' ', text)
#     return text.strip()

# # ============================================================
# # MAIN EXTRACTION FUNCTION (NO FALLBACK, NO POST‑PROCESSING)
# # ============================================================

# def extract_people_near_titles(html, debug=True):
#     blocks = extract_meaningful_blocks(html)
#     if debug:
#         print("\n[DEBUG] BLOCK COUNT:", len(blocks))
#         for i, block in enumerate(blocks[:5]):
#             print(f"\n--- BLOCK {i} ---")
#             print(block[:500])

#     results = defaultdict(list)  # title -> list of (person, confidence, distance)

#     total_blocks = len(blocks)
#     total_persons = 0
#     total_titles = 0
#     total_relations = 0

#     for block_idx, block in enumerate(blocks):
#         block_clean = clean_code_lines(block) if len(block.splitlines()) > 5 else block

#         # GLiNER extraction (includes confidence)
#         persons, titles = extract_entities_gliner(block_clean, threshold=0.35)

#         # Filter by confidence / validity
#         persons = [p for p in persons if is_valid_person(p)]
#         titles = [t for t in titles if is_valid_title(t)]

#         total_persons += len(persons)
#         total_titles += len(titles)

#         if debug:
#             print(f"\n[DEBUG] Block {block_idx+1} PERSONS:")
#             if persons:
#                 for p in persons:
#                     print(f"  {p['text']} (start={p['start']}, end={p['end']}, score={p['score']:.2f})")
#             else:
#                 print("  (No persons)")
#             print(f"[DEBUG] Block {block_idx+1} TITLES:")
#             if titles:
#                 for t in titles:
#                     print(f"  {t['text']} (start={t['start']}, end={t['end']}, score={t['score']:.2f})")
#             else:
#                 print("  (No titles)")

#         # Link persons to titles (new linker)
#         links = link_person_to_title(persons, titles, max_distance=60)
#         links = remove_duplicate_links(links)

#         total_relations += len(links)

#         if debug:
#             print(f"[DEBUG] Block {block_idx+1} LINKS:")
#             for link in links:
#                 print(f"  {link['person']} -> {link['title']} (distance={link['distance']}, confidence={link['confidence']:.2f})")

#         # Store results
#         for link in links:
#             results[link["title"]].append((link["person"], link["confidence"], link["distance"]))

#     if debug:
#         print("\n[DEBUG] SUMMARY:")
#         print(f"  Total blocks: {total_blocks}")
#         print(f"  Total persons: {total_persons}")
#         print(f"  Total titles: {total_titles}")
#         print(f"  Total person-title links: {total_relations}")

#     # Deduplicate per title: keep the highest confidence person (or best distance)
#     final = {}
#     for title, pairs in results.items():
#         # sort by confidence descending, then by distance ascending
#         sorted_pairs = sorted(pairs, key=lambda x: (-x[1], x[2]))
#         # keep unique persons (in case of duplicates)
#         seen_names = set()
#         unique = []
#         for name, conf, dist in sorted_pairs:
#             if name not in seen_names:
#                 seen_names.add(name)
#                 unique.append({"name": name, "confidence": conf, "distance": dist})
#         final[title] = unique

#     return final

# # ============================================================
# # MAIN
# # ============================================================

# if __name__ == "__main__":
#     print("Job Title + Person Extractor (GLiNER + Confidence + Linker + BFS Crawler)")
#     print("Paste a URL (starts with http) or raw HTML. Type exit when done.\n")

#     lines = []
#     while True:
#         line = input()
#         if line.strip().lower() == "exit":
#             break
#         lines.append(line)

#     user_input = "\n".join(lines).strip()

#     # Clean up common markdown/typo issues
#     if user_input.startswith("[") and "]" in user_input:
#         user_input = user_input.split("](")[-1].rstrip(")")

#     # Check if it's a URL
#     if user_input.startswith("http://") or user_input.startswith("https://"):
#         print(f"Starting full domain crawl for: {user_input}")

#         website_pages = crawl_domain_bfs(
#             user_input,
#             max_pages=100,
#             max_depth=3,
#             wait_for_time=3000
#         )

#         combined_results = defaultdict(list)

#         for url, html in website_pages.items():
#             print(f"\n\nPROCESSING PAGE: {url}")
#             page_results = extract_people_near_titles(html, debug=False)
#             for title, people in page_results.items():
#                 combined_results[title].extend(people)

#         print("\n=== FINAL SITE RESULTS ===\n")
#         if not combined_results:
#             print("No person-role relationships found.")
#         else:
#             for title, people in combined_results.items():
#                 print(f"Title: {title}")
#                 seen = set()
#                 for person in people:
#                     if person["name"] not in seen:
#                         seen.add(person["name"])
#                         print(f"  - {person['name']} (confidence={person['confidence']:.2f})")
#                 print()
#     else:
#         # Treat as raw HTML
#         html = user_input
#         if html:
#             results = extract_people_near_titles(html, debug=True)
#             print("\n=== RESULTS ===\n")
#             if not results:
#                 print("No person-role relationships found.")
#             else:
#                 for title, persons in results.items():
#                     print(f"Title: {title}")
#                     for entry in persons:
#                         print(f"  - {entry['name']} (confidence: {entry['confidence']:.2f}, distance: {entry['distance']})")
#                     print()
#         else:
#             print("No HTML to process.")





# """
# Job Title + Person Extractor (Optimized)
# ----------------------------------------------------------------------------
# - Fast parallel crawler using Scrapling StealthyFetcher (threaded)
# - Batched GLiNER extraction across all blocks (one model call per page)
# - Parallel page analysis with ThreadPoolExecutor
# - Torch inference optimization (eval, no_grad, GPU if available)
# - No hardcoded word filters, no fallback regex, no O*NET mapping
# """

# import re
# from collections import defaultdict, deque
# from bs4 import BeautifulSoup
# from sentence_transformers import SentenceTransformer
# import numpy as np
# import fasttext
# import os
# import requests
# from gliner import GLiNER
# from urllib.parse import urljoin, urlparse
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from queue import Queue
# import threading
# import time
# import torch

# # ============================================================
# # SCRAPLING FETCHER (fixed for Scrapling 0.4.14)
# # ============================================================

# try:
#     from scrapling.fetchers import StealthyFetcher
#     SCRAPLING_AVAILABLE = True
# except ImportError:
#     SCRAPLING_AVAILABLE = False
#     print("⚠️ StealthyFetcher not available. Falling back to requests.")

# def fetch_html(url, wait_for_time=0, scroll_to_bottom=False, scroll_delay=0):
#     """
#     Fetch HTML using Scrapling StealthyFetcher (no headless arg) or requests.
#     """
#     if SCRAPLING_AVAILABLE:
#         try:
#             response = StealthyFetcher.fetch(
#                 url,
#                 timeout=30000
#             )
#             if response is None:
#                 raise Exception("Empty response")
#             if hasattr(response, "html"):
#                 return response.html
#             return str(response)
#         except Exception as e:
#             print(f"[DEBUG] Scrapling error: {e}. Falling back to requests.")

#     # Requests fallback
#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.5",
#     })
#     response = session.get(url, timeout=30, allow_redirects=True)
#     response.raise_for_status()
#     return response.text

# # ============================================================
# # URL HELPERS
# # ============================================================

# def normalize_url(url):
#     parsed = urlparse(url)
#     return parsed._replace(fragment="").geturl().rstrip("/")

# def is_same_domain(url, base_domain):
#     parsed = urlparse(url)
#     return (parsed.netloc == base_domain or parsed.netloc.endswith("." + base_domain))

# def extract_links_from_html(html, current_url):
#     soup = BeautifulSoup(html, "html.parser")
#     links = []
#     for a in soup.find_all("a", href=True):
#         href = a["href"].strip()
#         if (href.startswith("#") or href.startswith("mailto:") or
#             href.startswith("tel:") or href.startswith("javascript:")):
#             continue
#         absolute = urljoin(current_url, href)
#         links.append(normalize_url(absolute))
#     return links

# # ============================================================
# # FAST PARALLEL CRAWLER (threaded queue)
# # ============================================================

# def fast_parallel_crawl(start_url, max_pages=100, max_depth=3, workers=8):
#     """
#     Multi-threaded BFS crawler using a queue.
#     """
#     start_url = normalize_url(start_url)
#     domain = urlparse(start_url).netloc

#     queue = Queue()
#     queue.put((start_url, 0))

#     visited = set()
#     pages = {}
#     lock = threading.Lock()

#     def fetch_worker():
#         while True:
#             try:
#                 url, depth = queue.get(timeout=3)
#             except:
#                 return

#             if depth > max_depth:
#                 queue.task_done()
#                 continue

#             with lock:
#                 if url in visited:
#                     queue.task_done()
#                     continue
#                 if len(visited) >= max_pages:
#                     queue.task_done()
#                     continue
#                 visited.add(url)

#             try:
#                 print(f"[FETCH] {url}")
#                 html = fetch_html(url, wait_for_time=0, scroll_to_bottom=False)

#                 with lock:
#                     pages[url] = html

#                 links = extract_links_from_html(html, url)
#                 for link in links:
#                     if (is_same_domain(link, domain) and link not in visited):
#                         queue.put((link, depth + 1))
#             except Exception as e:
#                 print(f"[FAILED] {url}: {e}")

#             finally:
#                 queue.task_done()

#     threads = []
#     for _ in range(workers):
#         t = threading.Thread(target=fetch_worker, daemon=True)
#         t.start()
#         threads.append(t)

#     queue.join()
#     print(f"CRAWLED: {len(pages)} pages")
#     return pages

# # ============================================================
# # LOAD FASTTEXT MODEL (for code removal)
# # ============================================================

# MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
# MODEL_PATH = "code_nl_model.bin"

# if not os.path.exists(MODEL_PATH):
#     print("Downloading FastText model...")
#     response = requests.get(MODEL_URL, stream=True)
#     with open(MODEL_PATH, "wb") as f:
#         for chunk in response.iter_content(chunk_size=1024 * 1024):
#             if chunk:
#                 f.write(chunk)
#     print("Model downloaded.")

# print("Loading FastText model...")
# fasttext_model = fasttext.load_model(MODEL_PATH)
# print("FastText model loaded.")

# def is_code_line(line, threshold=0.5):
#     if not line.strip():
#         return False
#     labels, probs = fasttext_model.predict(line.strip())
#     if labels[0] == "__label__code" and probs[0] > threshold:
#         return True
#     return False

# def clean_code_lines(text):
#     lines = text.splitlines()
#     return "\n".join([line for line in lines if not is_code_line(line)])

# # ============================================================
# # LOAD GLINER MODEL WITH TORCH OPTIMIZATIONS
# # ============================================================

# print("Loading GLiNER...")
# gliner_model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")

# # Torch inference optimizations
# gliner_model.eval()
# torch.set_grad_enabled(False)

# if torch.cuda.is_available():
#     gliner_model.to("cuda")
#     print("GLiNER loaded on GPU")
# else:
#     print("GLiNER loaded on CPU")

# # ============================================================
# # HTML BLOCK EXTRACTION
# # ============================================================

# def extract_meaningful_blocks(html, min_words=1):
#     soup = BeautifulSoup(html, "html.parser")
#     for tag in soup(["script", "style", "nav", "footer", "header"]):
#         tag.decompose()
#     blocks = []
#     candidates = soup.find_all(["div", "section", "article", "li"])
#     for element in candidates:
#         text = element.get_text(" ", strip=True)
#         words = text.split()
#         if min_words <= len(words) <= 80:
#             blocks.append(text)
#     seen = set()
#     result = []
#     for b in blocks:
#         if b not in seen:
#             seen.add(b)
#             result.append(b)
#     return result

# # ============================================================
# # GLiNER EXTRACTION (single call)
# # ============================================================

# def extract_entities_gliner(text, threshold=0.35):
#     labels = ["person", "job title"]
#     entities = gliner_model.predict_entities(text, labels, threshold=threshold)
#     persons = []
#     titles = []
#     for e in entities:
#         item = {
#             "text": e["text"].strip(),
#             "start": e["start"],
#             "end": e["end"],
#             "score": e.get("score", 0.0)
#         }
#         if e["label"] == "person":
#             persons.append(item)
#         elif e["label"] == "job title":
#             titles.append(item)
#     return persons, titles

# # ============================================================
# # VALIDATION BY CONFIDENCE ONLY
# # ============================================================

# def is_valid_person(entity, min_confidence=0.55):
#     text = entity["text"].strip()
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     parts = text.split()
#     if len(parts) < 2:
#         return False
#     if any(c in text.lower().split() for c in ["&", "and"]):
#         return False
#     return True

# def is_valid_title(entity, min_confidence=0.45):
#     score = entity.get("score", 0)
#     if score < min_confidence:
#         return False
#     return True

# # ============================================================
# # LINKER – FIND CLOSEST TITLE FOR EACH PERSON
# # ============================================================

# def link_person_to_title(persons, titles, max_distance=60):
#     relations = []
#     for person in persons:
#         best_title = None
#         best_distance = 9999
#         for title in titles:
#             if (person["start"] >= title["start"] and person["start"] <= title["end"]) or \
#                (title["start"] >= person["start"] and title["start"] <= person["end"]):
#                 continue
#             distance = abs(person["start"] - title["end"])
#             if distance < best_distance:
#                 best_distance = distance
#                 best_title = title
#         if best_title and best_distance <= max_distance:
#             relations.append({
#                 "person": person["text"],
#                 "title": best_title["text"],
#                 "distance": best_distance,
#                 "confidence": (person.get("score", 0) + best_title.get("score", 0)) / 2
#             })
#     return relations

# def remove_duplicate_links(links):
#     seen = set()
#     clean = []
#     for link in links:
#         key = (link["person"], link["title"])
#         if key not in seen:
#             seen.add(key)
#             clean.append(link)
#     return clean

# # ============================================================
# # MAIN EXTRACTION FUNCTION – BATCHED GLINER
# # ============================================================

# def extract_people_near_titles(html, debug=True):
#     blocks = extract_meaningful_blocks(html)
#     if debug:
#         print("\n[DEBUG] BLOCK COUNT:", len(blocks))
#         for i, block in enumerate(blocks[:5]):
#             print(f"\n--- BLOCK {i} ---")
#             print(block[:500])

#     if not blocks:
#         return {}

#     # BATCH: run GLiNER once on ALL blocks concatenated
#     separator = "\n---BLOCK_SEPARATOR---\n"
#     concatenated = separator.join(blocks)
#     all_persons, all_titles = extract_entities_gliner(concatenated, threshold=0.35)

#     # Map entities back to blocks using boundaries
#     boundaries = []
#     pos = 0
#     for block in blocks:
#         boundaries.append(pos)
#         pos += len(block) + len(separator)
#     boundaries.append(pos)

#     def map_entities(entities, boundaries):
#         block_results = [[] for _ in range(len(blocks))]
#         for ent in entities:
#             start = ent["start"]
#             block_idx = 0
#             for i in range(len(boundaries)-1):
#                 if start >= boundaries[i] and start < boundaries[i+1]:
#                     block_idx = i
#                     break
#             ent["start"] -= boundaries[block_idx]
#             ent["end"] -= boundaries[block_idx]
#             block_results[block_idx].append(ent)
#         return block_results

#     persons_per_block = map_entities(all_persons, boundaries)
#     titles_per_block = map_entities(all_titles, boundaries)

#     results = defaultdict(list)
#     total_persons = 0
#     total_titles = 0
#     total_relations = 0

#     for block_idx, (persons, titles) in enumerate(zip(persons_per_block, titles_per_block)):
#         persons = [p for p in persons if is_valid_person(p)]
#         titles = [t for t in titles if is_valid_title(t)]

#         total_persons += len(persons)
#         total_titles += len(titles)

#         if debug:
#             print(f"\n[DEBUG] Block {block_idx+1} PERSONS:")
#             if persons:
#                 for p in persons:
#                     print(f"  {p['text']} (start={p['start']}, end={p['end']}, score={p['score']:.2f})")
#             else:
#                 print("  (No persons)")
#             print(f"[DEBUG] Block {block_idx+1} TITLES:")
#             if titles:
#                 for t in titles:
#                     print(f"  {t['text']} (start={t['start']}, end={t['end']}, score={t['score']:.2f})")
#             else:
#                 print("  (No titles)")

#         links = link_person_to_title(persons, titles, max_distance=60)
#         links = remove_duplicate_links(links)

#         total_relations += len(links)

#         if debug:
#             print(f"[DEBUG] Block {block_idx+1} LINKS:")
#             for link in links:
#                 print(f"  {link['person']} -> {link['title']} (distance={link['distance']}, confidence={link['confidence']:.2f})")

#         for link in links:
#             results[link["title"]].append((link["person"], link["confidence"], link["distance"]))

#     if debug:
#         print("\n[DEBUG] SUMMARY:")
#         print(f"  Total blocks: {len(blocks)}")
#         print(f"  Total persons: {total_persons}")
#         print(f"  Total titles: {total_titles}")
#         print(f"  Total person-title links: {total_relations}")

#     final = {}
#     for title, pairs in results.items():
#         sorted_pairs = sorted(pairs, key=lambda x: (-x[1], x[2]))
#         seen_names = set()
#         unique = []
#         for name, conf, dist in sorted_pairs:
#             if name not in seen_names:
#                 seen_names.add(name)
#                 unique.append({"name": name, "confidence": conf, "distance": dist})
#         final[title] = unique

#     return final

# # ============================================================
# # MAIN
# # ============================================================

# if __name__ == "__main__":
#     print("Job Title + Person Extractor (Optimized)")
#     print("Paste a URL (starts with http) or raw HTML. Type exit when done.\n")

#     lines = []
#     while True:
#         line = input()
#         if line.strip().lower() == "exit":
#             break
#         lines.append(line)

#     user_input = "\n".join(lines).strip()

#     # Clean up markdown/typo issues
#     if user_input.startswith("[") and "]" in user_input:
#         user_input = user_input.split("](")[-1].rstrip(")")

#     if user_input.startswith("http://") or user_input.startswith("https://"):
#         print(f"Starting full domain crawl for: {user_input}")

#         # Fast parallel crawler
#         website_pages = fast_parallel_crawl(
#             user_input,
#             max_pages=200,
#             max_depth=3,
#             workers=8
#         )

#         # Parallel page analysis with ThreadPoolExecutor
#         combined_results = defaultdict(list)

#         def analyze_page(item):
#             url, html = item
#             print(f"[ANALYZE] {url}")
#             return extract_people_near_titles(html, debug=False)

#         with ThreadPoolExecutor(max_workers=4) as executor:
#             futures = [executor.submit(analyze_page, item) for item in website_pages.items()]
#             for future in as_completed(futures):
#                 result = future.result()
#                 for title, people in result.items():
#                     combined_results[title].extend(people)

#         print("\n=== FINAL SITE RESULTS ===\n")
#         if not combined_results:
#             print("No person-role relationships found.")
#         else:
#             for title, people in combined_results.items():
#                 print(f"Title: {title}")
#                 seen = set()
#                 for person in people:
#                     if person["name"] not in seen:
#                         seen.add(person["name"])
#                         print(f"  - {person['name']} (confidence={person['confidence']:.2f})")
#                 print()
#     else:
#         html = user_input
#         if html:
#             results = extract_people_near_titles(html, debug=True)
#             print("\n=== RESULTS ===\n")
#             if not results:
#                 print("No person-role relationships found.")
#             else:
#                 for title, persons in results.items():
#                     print(f"Title: {title}")
#                     for entry in persons:
#                         print(f"  - {entry['name']} (confidence: {entry['confidence']:.2f}, distance: {entry['distance']})")
#                     print()
#         else:
#             print("No HTML to process.")



"""
Job Title + Person Extractor (Optimized with Crawler Debug)
----------------------------------------------------------------------------
- Fast parallel BFS crawler with priority scoring and JS wait
- Debug output to diagnose link discovery
- Batched GLiNER extraction (one call per page)
- Parallel page analysis with ThreadPoolExecutor
- GPU support with Torch
- Keeps header/footer content
"""

import re
from collections import defaultdict
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
import numpy as np
import fasttext
import os
import requests
from gliner import GLiNER
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import time
import torch

# ============================================================
# SCRAPLING FETCHER (with JS wait and debug)
# ============================================================

try:
    from scrapling.fetchers import StealthyFetcher
    SCRAPLING_AVAILABLE = True
except ImportError:
    SCRAPLING_AVAILABLE = False
    print("⚠️ StealthyFetcher not available. Falling back to requests.")

def fetch_html(url, wait_for_time=3000):
    """
    Fetch HTML using Scrapling StealthyFetcher with wait for JS.
    """
    if SCRAPLING_AVAILABLE:
        try:
            print(f"[SCRAPLING] {url}")
            response = StealthyFetcher.fetch(url, wait=wait_for_time)
            if response is None:
                raise Exception("Empty response")
            html = response.html
            print(f"HTML LENGTH: {len(html)}")
            print(f"HTML PREVIEW: {html[:500]}...")
            return html
        except Exception as e:
            print(f"[SCRAPLING ERROR] {e}")

    # Requests fallback
    print(f"[REQUESTS] {url}")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    response = session.get(url, timeout=30, allow_redirects=True)
    response.raise_for_status()
    html = response.text
    print(f"HTML LENGTH: {len(html)}")
    print(f"HTML PREVIEW: {html[:500]}...")
    return html

# ============================================================
# URL HELPERS
# ============================================================

def normalize_url(url):
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl().rstrip("/")

def is_same_domain(url, base_domain):
    parsed = urlparse(url)
    return (parsed.netloc == base_domain or parsed.netloc.endswith("." + base_domain))

def extract_links_from_html(html, current_url):
    """
    Extract internal links from rendered HTML (including <link> tags).
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for tag in soup.find_all(["a", "link"], href=True):
        href = tag["href"].strip()
        if (href.startswith("#") or href.startswith("mailto:") or
            href.startswith("tel:") or href.startswith("javascript:")):
            continue
        absolute = urljoin(current_url, href)
        links.append(normalize_url(absolute))
    # Remove duplicates
    return list(set(links))

# ============================================================
# PRIORITY CRAWLER (concurrent BFS with debug)
# ============================================================

PRIORITY_WORDS = ["team", "leadership", "about", "management", "executive", "people"]

def url_priority_score(url):
    """Score URL by relevance to team/leadership content."""
    url_lower = url.lower()
    score = 0
    for w in PRIORITY_WORDS:
        if w in url_lower:
            score += 1
    return score

def parallel_crawl(start_url, max_pages=200, max_depth=3, workers=8):
    """
    Concurrent BFS crawler with priority scoring.
    """
    start_url = normalize_url(start_url)
    domain = urlparse(start_url).netloc

    queue = Queue()
    queue.put((start_url, 0))

    visited = set()
    pages = {}
    lock = threading.Lock()

    def worker():
        while True:
            item = queue.get()
            if item is None:
                break

            url, depth = item

            with lock:
                if url in visited:
                    queue.task_done()
                    continue
                visited.add(url)

            print(f"[FETCH] {url} (depth={depth})")

            try:
                html = fetch_html(url, wait_for_time=3000)  # 3 sec wait for JS
                if html is None or len(html) < 100:
                    print(f"[FAILED] {url} - empty or too short")
                    queue.task_done()
                    continue

                with lock:
                    pages[url] = html

                links = extract_links_from_html(html, url)
                print(f"FOUND LINKS: {len(links)}")
                for x in links[:10]:
                    print(f"  -> {x}")

                # Sort links by priority before adding to queue
                priority_links = []
                normal_links = []
                for link in links:
                    if len(pages) >= max_pages:
                        break
                    if not is_same_domain(link, domain):
                        continue
                    if link in visited:
                        continue
                    if depth >= max_depth:
                        continue
                    if url_priority_score(link) > 0:
                        priority_links.append((link, depth + 1))
                    else:
                        normal_links.append((link, depth + 1))

                # Add priority links first
                for link, d in priority_links:
                    if len(pages) >= max_pages:
                        break
                    queue.put((link, d))

                # Then normal links
                for link, d in normal_links:
                    if len(pages) >= max_pages:
                        break
                    queue.put((link, d))

            except Exception as e:
                print(f"[FAILED] {url}: {e}")

            queue.task_done()

    threads = []
    for _ in range(workers):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    queue.join()

    # Signal workers to stop
    for _ in threads:
        queue.put(None)

    for t in threads:
        t.join()

    print(f"\nCRAWLED: {len(pages)} pages")
    return pages

# ============================================================
# LOAD FASTTEXT MODEL (for code removal)
# ============================================================

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
MODEL_PATH = "code_nl_model.bin"

if not os.path.exists(MODEL_PATH):
    print("Downloading FastText model...")
    response = requests.get(MODEL_URL, stream=True)
    with open(MODEL_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    print("Model downloaded.")

print("Loading FastText model...")
fasttext_model = fasttext.load_model(MODEL_PATH)
print("FastText model loaded.")

def is_code_line(line, threshold=0.5):
    if not line.strip():
        return False
    labels, probs = fasttext_model.predict(line.strip())
    if labels[0] == "__label__code" and probs[0] > threshold:
        return True
    return False

def clean_code_lines(text):
    lines = text.splitlines()
    return "\n".join([line for line in lines if not is_code_line(line)])

# ============================================================
# LOAD GLINER MODEL WITH GPU SUPPORT
# ============================================================

print("Loading GLiNER...")
gliner_model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")

# Torch optimizations
gliner_model.eval()
torch.set_grad_enabled(False)

device = "cuda" if torch.cuda.is_available() else "cpu"
if torch.cuda.is_available():
    gliner_model.to("cuda")
    print(f"GLiNER loaded on GPU ({device})")
else:
    print("GLiNER loaded on CPU")

# ============================================================
# HTML BLOCK EXTRACTION (KEEP header/footer)
# ============================================================

def extract_meaningful_blocks(html, min_words=1):
    soup = BeautifulSoup(html, "html.parser")
    # Remove only script/style/noscript
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    blocks = []
    candidates = soup.find_all(["div", "section", "article", "li", "header", "footer"])
    for element in candidates:
        text = element.get_text(" ", strip=True)
        words = text.split()
        if min_words <= len(words) <= 80:
            blocks.append(text)
    seen = set()
    result = []
    for b in blocks:
        if b not in seen:
            seen.add(b)
            result.append(b)
    return result

# ============================================================
# GLiNER EXTRACTION
# ============================================================

def extract_entities_gliner(text, threshold=0.35):
    labels = ["person", "job title"]
    entities = gliner_model.predict_entities(text, labels, threshold=threshold)
    persons = []
    titles = []
    for e in entities:
        item = {
            "text": e["text"].strip(),
            "start": e["start"],
            "end": e["end"],
            "score": e.get("score", 0.0)
        }
        if e["label"] == "person":
            persons.append(item)
        elif e["label"] == "job title":
            titles.append(item)
    return persons, titles

# ============================================================
# VALIDATION BY CONFIDENCE ONLY
# ============================================================

def is_valid_person(entity, min_confidence=0.55):
    text = entity["text"].strip()
    score = entity.get("score", 0)
    if score < min_confidence:
        return False
    parts = text.split()
    if len(parts) < 2:
        return False
    if any(c in text.lower().split() for c in ["&", "and"]):
        return False
    return True

def is_valid_title(entity, min_confidence=0.45):
    score = entity.get("score", 0)
    if score < min_confidence:
        return False
    return True

# ============================================================
# LINKER
# ============================================================

def link_person_to_title(persons, titles, max_distance=60):
    relations = []
    for person in persons:
        best_title = None
        best_distance = 9999
        for title in titles:
            if (person["start"] >= title["start"] and person["start"] <= title["end"]) or \
               (title["start"] >= person["start"] and title["start"] <= person["end"]):
                continue
            distance = abs(person["start"] - title["end"])
            if distance < best_distance:
                best_distance = distance
                best_title = title
        if best_title and best_distance <= max_distance:
            relations.append({
                "person": person["text"],
                "title": best_title["text"],
                "distance": best_distance,
                "confidence": (person.get("score", 0) + best_title.get("score", 0)) / 2
            })
    return relations

def remove_duplicate_links(links):
    seen = set()
    clean = []
    for link in links:
        key = (link["person"], link["title"])
        if key not in seen:
            seen.add(key)
            clean.append(link)
    return clean

# ============================================================
# MAIN EXTRACTION FUNCTION – BATCHED GLINER
# ============================================================

def extract_people_near_titles(html, debug=True):
    blocks = extract_meaningful_blocks(html)
    if debug:
        print(f"\n[DEBUG] BLOCK COUNT: {len(blocks)}")
        for i, block in enumerate(blocks[:3]):
            print(f"\n--- BLOCK {i} ---")
            print(block[:300])

    if not blocks:
        return {}

    # BATCH: run GLiNER once on ALL blocks concatenated
    separator = "\n---BLOCK_SEPARATOR---\n"
    concatenated = separator.join(blocks)
    all_persons, all_titles = extract_entities_gliner(concatenated, threshold=0.35)

    # Map entities back to blocks using boundaries
    boundaries = []
    pos = 0
    for block in blocks:
        boundaries.append(pos)
        pos += len(block) + len(separator)
    boundaries.append(pos)

    def map_entities(entities, boundaries):
        block_results = [[] for _ in range(len(blocks))]
        for ent in entities:
            start = ent["start"]
            block_idx = 0
            for i in range(len(boundaries)-1):
                if start >= boundaries[i] and start < boundaries[i+1]:
                    block_idx = i
                    break
            ent["start"] -= boundaries[block_idx]
            ent["end"] -= boundaries[block_idx]
            block_results[block_idx].append(ent)
        return block_results

    persons_per_block = map_entities(all_persons, boundaries)
    titles_per_block = map_entities(all_titles, boundaries)

    results = defaultdict(list)

    for block_idx, (persons, titles) in enumerate(zip(persons_per_block, titles_per_block)):
        persons = [p for p in persons if is_valid_person(p)]
        titles = [t for t in titles if is_valid_title(t)]

        if debug:
            print(f"\n[DEBUG] Block {block_idx+1} PERSONS: {len(persons)}, TITLES: {len(titles)}")

        links = link_person_to_title(persons, titles, max_distance=60)
        links = remove_duplicate_links(links)

        for link in links:
            results[link["title"]].append((link["person"], link["confidence"], link["distance"]))

    # Deduplicate per title
    final = {}
    for title, pairs in results.items():
        sorted_pairs = sorted(pairs, key=lambda x: (-x[1], x[2]))
        seen_names = set()
        unique = []
        for name, conf, dist in sorted_pairs:
            if name not in seen_names:
                seen_names.add(name)
                unique.append({"name": name, "confidence": conf, "distance": dist})
        final[title] = unique

    return final

# ============================================================
# ANALYZE PAGE (for parallel execution)
# ============================================================

def analyze_page(item):
    url, html = item
    print(f"[ANALYZE] {url}")
    return extract_people_near_titles(html, debug=False)

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Job Title + Person Extractor (Optimized)")
    print("Paste a URL (starts with http) or raw HTML. Type exit when done.\n")

    lines = []
    while True:
        line = input()
        if line.strip().lower() == "exit":
            break
        lines.append(line)

    user_input = "\n".join(lines).strip()

    # Clean up markdown/typo issues
    if user_input.startswith("[") and "]" in user_input:
        user_input = user_input.split("](")[-1].rstrip(")")

    if user_input.startswith("http://") or user_input.startswith("https://"):
        print(f"\nStarting full domain crawl for: {user_input}")

        # Parallel crawler with priority
        website_pages = parallel_crawl(
            user_input,
            max_pages=200,
            max_depth=3,
            workers=8
        )

        if not website_pages:
            print("No pages crawled.")
            exit()

        # Parallel page analysis
        combined_results = defaultdict(list)

        print("\nAnalyzing pages in parallel...")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(analyze_page, item) for item in website_pages.items()]
            for future in as_completed(futures):
                result = future.result()
                for title, people in result.items():
                    combined_results[title].extend(people)

        print("\n=== FINAL SITE RESULTS ===\n")
        if not combined_results:
            print("No person-role relationships found.")
        else:
            for title, people in combined_results.items():
                print(f"Title: {title}")
                seen = set()
                for person in people:
                    if person["name"] not in seen:
                        seen.add(person["name"])
                        print(f"  - {person['name']} (confidence={person['confidence']:.2f})")
                print()
    else:
        html = user_input
        if html:
            results = extract_people_near_titles(html, debug=True)
            print("\n=== RESULTS ===\n")
            if not results:
                print("No person-role relationships found.")
            else:
                for title, persons in results.items():
                    print(f"Title: {title}")
                    for entry in persons:
                        print(f"  - {entry['name']} (confidence: {entry['confidence']:.2f}, distance: {entry['distance']})")
                    print()
        else:
            print("No HTML to process.")