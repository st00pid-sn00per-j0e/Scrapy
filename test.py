import re
import sys
import requests
from html import unescape
from urllib.parse import unquote
import time

# ---- Page fetching ----
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def fetch_page(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    resp = requests.get(url, timeout=15, headers=HEADERS)
    resp.raise_for_status()
    return resp.text

def clean_html(raw):
    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = unquote(text)
    text = text.replace("%40", "@")
    return text

# ---- Model loading ----
def load_flan():
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    print("Loading flan-t5-base ...")
    tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-base")
    model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-base")
    return tokenizer, model

def load_phi2():
    from transformers import AutoTokenizer, AutoModelForCausalLM
    print("Loading phi-2 (this may take a while on CPU) ...")
    tokenizer = AutoTokenizer.from_pretrained("microsoft/phi-2", trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        "microsoft/phi-2",
        trust_remote_code=True,
        torch_dtype="auto"  # will load in fp16 if supported, else fp32
    )
    return tokenizer, model

# ---- Extraction functions ----
def extract_with_flan(tokenizer, model, snippet):
    prompt = (
        "Extract all email addresses and phone numbers from the following HTML.\n"
        f"HTML: {snippet}\n"
        "Answer with a simple list. If none, say 'none'."
    )
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
    outputs = model.generate(**inputs, max_new_tokens=200)
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return result

def extract_with_phi2(tokenizer, model, snippet):
    # Phi-2 is a causal LM, we need to construct a chat-like prompt
    prompt = f"""Instruct: Extract all email addresses and phone numbers from the following HTML code.
Output them as a list. If none, say 'none'.

HTML:
{snippet}

Output:"""
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
    # Generate with a limited number of new tokens
    outputs = model.generate(
        **inputs,
        max_new_tokens=200,
        temperature=0.2,
        do_sample=True,
        pad_token_id=tokenizer.eos_token_id
    )
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    # Remove the prompt part from the output
    if result.startswith(prompt):
        result = result[len(prompt):].strip()
    return result

def regex_fallback(text):
    emails = set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}", text))
    phones = set(re.findall(r"\+?\d[\d\-\(\)\s]{6,}\d", text))
    return sorted(emails), sorted(phones)

def print_results(model_name, raw_output):
    print(f"\n--- {model_name} raw output ---")
    print(raw_output if raw_output else "(empty)")

    emails, phones = regex_fallback(raw_output)
    if emails or phones:
        print(f"\n--- Regex-extracted from {model_name} output ---")
        if emails:
            print("Emails:", emails)
        if phones:
            print("Phones:", phones)
    else:
        print("No structured emails/phones found in output.")

# ---- Main ----
def main():
    # Ask which model(s) to use
    choice = input("Choose model(s) - 1: flan-t5-base, 2: phi-2, 3: both: ").strip()
    use_flan = choice in ("1", "3")
    use_phi  = choice in ("2", "3")

    if not use_flan and not use_phi:
        print("No model selected. Exiting.")
        return

    url = input("Enter website URL: ").strip()
    if not url:
        print("No URL provided.")
        return

    print("Fetching page source...")
    try:
        source = fetch_page(url)
    except Exception as e:
        print(f"Error fetching page: {e}")
        return

    # Prepare snippet (first 1500 characters, but keep whole for regex if needed)
    snippet = clean_html(source)[:1500]  # limit to avoid blowing context window

    if use_flan:
        tokenizer, model = load_flan()
        start = time.time()
        raw = extract_with_flan(tokenizer, model, snippet)
        elapsed = time.time() - start
        print(f"Flan‑T5 extraction took {elapsed:.1f}s")
        print_results("Flan‑T5‑base", raw)

    if use_phi:
        tokenizer, model = load_phi2()
        start = time.time()
        raw = extract_with_phi2(tokenizer, model, snippet)
        elapsed = time.time() - start
        print(f"Phi‑2 extraction took {elapsed:.1f}s")
        print_results("Phi‑2", raw)

    # Also show standard regex extraction for comparison
    print("\n===== Standard regex extraction (no LLM) =====")
    emails = set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}", source))
    phones = set(re.findall(r"\+?\d[\d\-\(\)\s]{6,}\d", clean_html(source)))
    if emails:
        print("Emails:", sorted(emails))
    else:
        print("Emails: None")
    if phones:
        print("Phones:", sorted(phones))
    else:
        print("Phones: None")

if __name__ == "__main__":
    main()