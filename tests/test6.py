import os
import time
import random
import warnings
import concurrent.futures
from typing import Dict, Any
from datetime import datetime

import g4f
from g4f import Provider
from g4f.client import Client

# Suppress unclosed session warnings
warnings.filterwarnings("ignore", message="Unclosed client session")

# ============================================================
# CONFIG
# ============================================================

# All providers we want to test (including those that previously failed)
PROVIDER_NAMES = [
    "CohereForAI_C4AI_Command",
    "Cloudflare",
    "AnyProvider",
    "Gemini",
    "Perplexity",
    "Yqcloud",
    "OpenaiChat",
    "MiniMax",
    "WhiteRabbitNeo",
]

TEST_PROMPT = "Hi! Please reply with a short greeting and your name (e.g., 'Hello, I am Gemini')."
MAX_RETRIES = 3          # more retries for stubborn providers
RETRY_DELAY = 2
TIMEOUT = 30
MAX_WORKERS = 5          # adjust as needed

# Output folder
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# HELPER: Get provider class
# ============================================================

def get_provider_class(name: str):
    return getattr(Provider, name, None)

# ============================================================
# HELPER: Save response to file
# ============================================================

def save_response(provider_name: str, content: str, success: bool, error: str = ""):
    """Save the response (or error) to a text file in the output folder."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{provider_name}_{timestamp}.txt"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"Provider: {provider_name}\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Success: {success}\n")
        if success:
            f.write("\n--- RESPONSE ---\n")
            f.write(content)
        else:
            f.write("\n--- ERROR ---\n")
            f.write(error)
    return filepath

# ============================================================
# TEST A SINGLE PROVIDER (called in parallel)
# ============================================================

def test_provider(provider_name: str) -> Dict[str, Any]:
    provider_class = get_provider_class(provider_name)
    if not provider_class:
        return {
            "name": provider_name,
            "success": False,
            "error": "Provider class not found",
            "response": None,
        }

    # Determine a model
    model = None
    if hasattr(provider_class, "models") and provider_class.models:
        model = provider_class.models[0]
    else:
        # Try common models
        for m in ["gpt-3.5-turbo", "claude-3-haiku", "gemini-pro"]:
            model = m
            break

    if not model:
        return {
            "name": provider_name,
            "success": False,
            "error": "No model available",
            "response": None,
        }

    for attempt in range(1, MAX_RETRIES + 1):
        client = Client()
        try:
            response = client.chat.completions.create(
                model=model,
                provider=provider_class,
                messages=[{"role": "user", "content": TEST_PROMPT}],
                stream=False,
                timeout=TIMEOUT,
            )
            # Extract content
            content = None
            if hasattr(response, "choices") and response.choices:
                message = response.choices[0].message
                if hasattr(message, "content") and isinstance(message.content, str):
                    content = message.content
                else:
                    content = f"[Non-text response: {type(message)}]"
            else:
                content = str(response) if response else "[Empty response]"

            # If content is a non‑text indicator, treat as failure
            if content and isinstance(content, str) and not content.startswith("[Non-text"):
                return {
                    "name": provider_name,
                    "success": True,
                    "response": content.strip(),
                    "attempt": attempt,
                }
            else:
                return {
                    "name": provider_name,
                    "success": False,
                    "error": content if content else "Empty or invalid response",
                    "response": None,
                }

        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY + random.uniform(0, 1))
            else:
                return {
                    "name": provider_name,
                    "success": False,
                    "error": f"{type(e).__name__}: {e}",
                    "response": None,
                }
    return {"name": provider_name, "success": False, "error": "Max retries exceeded"}

# ============================================================
# MAIN: Run all providers concurrently
# ============================================================

def main():
    print("=" * 90)
    print("TESTING ALL PROVIDERS (CONCURRENT) – INCLUDING PREVIOUSLY FAILED ONES")
    print("=" * 90)
    print(f"Output folder: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Testing {len(PROVIDER_NAMES)} providers with up to {MAX_WORKERS} at a time...\n")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_provider = {
            executor.submit(test_provider, name): name
            for name in PROVIDER_NAMES
        }
        # Process as they complete
        for future in concurrent.futures.as_completed(future_to_provider):
            provider_name = future_to_provider[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                result = {
                    "name": provider_name,
                    "success": False,
                    "error": f"Unexpected: {e}",
                    "response": None,
                }
                results.append(result)

            # Print and save result
            if result["success"]:
                print(f"\n[OK] {result['name']} responded:")
                print(f"   {result['response']}")
                print(f"   (attempt {result['attempt']})")
                saved_path = save_response(result["name"], result["response"], True)
            else:
                print(f"\n[FAIL] {result['name']} failed: {result['error']}")
                saved_path = save_response(result["name"], "", False, result["error"])
            print(f"   Saved to: {saved_path}")

    # Summary
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    print(f"Total providers: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        print("\nSuccessful responses:")
        for r in successful:
            print(f"\n[{r['name']}]")
            print(f"  {r['response']}")
    if failed:
        print("\nFailed providers:")
        for r in failed:
            print(f"  {r['name']}: {r['error']}")

    print(f"\nAll outputs saved in '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    main()