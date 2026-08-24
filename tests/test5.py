import os
import asyncio
import time
import random
import warnings
from typing import List, Dict, Any
from datetime import datetime

import g4f
from g4f import Provider
from g4f.client import AsyncClient

# Suppress harmless warnings
warnings.filterwarnings("ignore", message="Unclosed client session")

# ============================================================
# CONFIG
# ============================================================

TEST_PROMPT = "Reply with exactly: MODEL_TEST_SUCCESS"
MAX_RETRIES = 2
BASE_DELAY = 0.5          # seconds
TIMEOUT = 12              # seconds per attempt
MAX_CONCURRENT = 15       # number of concurrent tasks
OUTPUT_DIR = "output"

# Providers known to be non-chat (we skip them)
NON_CHAT_KEYWORDS = (
    "image", "audio", "tts", "search", "vision", "media",
    "create", "design", "flux", "sd3", "stability"
)

# Base / utility / account providers to skip
SKIP_NAMES = (
    "BaseProvider", "AsyncProvider", "AsyncGeneratorProvider",
    "IterListProvider", "RetryProvider", "RotatedProvider",
    "ProviderUtils", "Custom", "AnyProvider",
    "CopilotSession", "CopilotAccount", "CopilotApp",
    "MetaAIAccount", "OpenaiAccount", "OpenaiTemplate",
    "ClaudeAccount", "GeminiAccount", "OpenaiAPI",
)

# ============================================================
# AUTO-DISCOVER PROVIDERS
# ============================================================

def get_chat_providers() -> List[Any]:
    """Return provider classes that are likely chat-capable."""
    providers = []
    for name in sorted(dir(Provider)):
        if name.startswith("_"):
            continue
        attr = getattr(Provider, name)
        if not isinstance(attr, type):
            continue
        # Skip non-chat keywords
        if any(kw in name.lower() for kw in NON_CHAT_KEYWORDS):
            continue
        # Skip base/utility classes
        if name in SKIP_NAMES:
            continue
        # Real providers usually have a url attribute
        if not hasattr(attr, "url") or not attr.url:
            continue
        providers.append(attr)
    return providers

# ============================================================
# SAVE RESPONSE TO FILE
# ============================================================

def save_response(provider_name: str, content: str, success: bool, error: str = ""):
    """Save response or error to a text file in output folder."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
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
# ASYNC TEST A SINGLE PROVIDER (with retries)
# ============================================================

async def test_provider(
    client: AsyncClient,
    provider_class: Any,
    sem: asyncio.Semaphore,
    progress: Dict[str, Any]
) -> Dict[str, Any]:
    provider_name = provider_class.__name__

    async with sem:
        # Determine a model to use
        model = None
        if hasattr(provider_class, "models") and provider_class.models:
            model = provider_class.models[0]
        elif hasattr(provider_class, "default_model") and provider_class.default_model:
            model = provider_class.default_model
        else:
            # Fallback models - try the most common free ones
            fallbacks = ["gpt-4o-mini", "gpt-3.5-turbo", "claude-3-haiku", "llama-3.1-8b"]
            model = fallbacks[0]

        if not model:
            result = {
                "name": provider_name,
                "success": False,
                "error": "No model available",
                "response": None,
            }
            _update_progress(progress, result)
            return result

        for attempt in range(1, MAX_RETRIES + 1):
            start_time = time.time()
            try:
                # Use asyncio.wait_for for hard timeout enforcement
                response = await asyncio.wait_for(
                    client.chat.completions.create(
                        model=model,
                        provider=provider_class,
                        messages=[{"role": "user", "content": TEST_PROMPT}],
                        stream=False,
                    ),
                    timeout=TIMEOUT,
                )

                # Extract content – handle non-string responses
                content = None
                if hasattr(response, "choices") and response.choices:
                    message = response.choices[0].message
                    if hasattr(message, "content") and isinstance(message.content, str):
                        content = message.content
                    else:
                        content = f"[Non-text response: {type(message)}]"
                else:
                    content = str(response) if response else "[Empty response]"

                elapsed = time.time() - start_time

                if content and isinstance(content, str) and not content.startswith("[Non-text") and content != "[Empty response]":
                    result = {
                        "name": provider_name,
                        "success": True,
                        "response": content.strip(),
                        "time": elapsed,
                        "attempt": attempt,
                    }
                    _update_progress(progress, result)
                    save_response(result["name"], result["response"], True)
                    return result
                else:
                    result = {
                        "name": provider_name,
                        "success": False,
                        "error": content if content else "Empty or invalid response",
                        "response": None,
                        "attempt": attempt,
                    }
                    if attempt == MAX_RETRIES:
                        _update_progress(progress, result)
                        save_response(result["name"], "", False, result.get("error", "Unknown"))
                    return result

            except asyncio.TimeoutError:
                if attempt >= MAX_RETRIES:
                    result = {
                        "name": provider_name,
                        "success": False,
                        "error": f"Timeout after {TIMEOUT}s",
                        "response": None,
                        "attempt": attempt,
                    }
                    _update_progress(progress, result)
                    save_response(result["name"], "", False, result.get("error", "Unknown"))
                    return result
                await asyncio.sleep(BASE_DELAY * attempt)
            except Exception as e:
                if attempt >= MAX_RETRIES:
                    result = {
                        "name": provider_name,
                        "success": False,
                        "error": f"{type(e).__name__}: {e}",
                        "response": None,
                        "attempt": attempt,
                    }
                    _update_progress(progress, result)
                    save_response(result["name"], "", False, result.get("error", "Unknown"))
                    return result
                # Exponential backoff with jitter
                sleep_time = BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
                await asyncio.sleep(sleep_time)

        result = {"name": provider_name, "success": False, "error": "Max retries exceeded"}
        _update_progress(progress, result)
        save_response(result["name"], "", False, result.get("error", "Unknown"))
        return result

def _update_progress(progress: Dict[str, Any], result: Dict[str, Any]):
    """Print progress update."""
    progress["completed"] += 1
    status = "✅" if result["success"] else "❌"
    print(f"{status} {result['name']:<35} "
          f"{'OK' if result['success'] else 'FAIL':<6} "
          f"({progress['completed']}/{progress['total']})")

# ============================================================
# RUN ALL PROVIDERS
# ============================================================

async def run_all(providers: List[Any]) -> List[Dict]:
    client = AsyncClient()
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    progress = {"completed": 0, "total": len(providers)}

    # Create all tasks
    tasks = [
        asyncio.create_task(test_provider(client, p, sem, progress))
        for p in providers
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert any stray exceptions to result dicts
    processed = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            provider_name = providers[i].__name__ if i < len(providers) else "Unknown"
            processed.append({
                "name": provider_name,
                "success": False,
                "error": f"Unhandled {type(r).__name__}: {r}",
                "response": None,
            })
        else:
            processed.append(r)

    # Cleanup client if possible
    if hasattr(client, "close"):
        close_fn = client.close
        if asyncio.iscoroutinefunction(close_fn):
            try:
                await close_fn()
            except Exception:
                pass
        else:
            try:
                close_fn()
            except Exception:
                pass

    return processed

# ============================================================
# DISPLAY RESULTS
# ============================================================

def display_results(results: List[Dict]):
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    print("\n" + "=" * 90)
    print("ALL PROVIDERS TEST RESULTS")
    print("=" * 90)
    print(f"Total tested : {len(results)}")
    print(f"Successful   : {len(successful)}")
    print(f"Failed       : {len(failed)}")

    if successful:
        print("\n" + "-" * 90)
        print("SUCCESSFUL PROVIDERS (with response preview)")
        print("-" * 90)
        for i, r in enumerate(successful, 1):
            resp = r.get('response', '')
            preview = resp[:150] + ("..." if len(resp) > 150 else "")
            print(f"{i:2}. {r['name']:<35} "
                  f"time: {r.get('time', 0):.2f}s "
                  f"(attempt {r.get('attempt', 1)})")
            print(f"    Response: {preview!r}")

    if failed:
        print("\n" + "-" * 90)
        print("FAILED PROVIDERS")
        print("-" * 90)
        for i, r in enumerate(failed, 1):
            print(f"{i:2}. {r['name']}")
            print(f"    Error: {r.get('error', 'Unknown')}")

    print("\n" + "=" * 90)
    print("WORKING PROVIDERS - PYTHON LIST")
    print("=" * 90)
    working_names = [r['name'] for r in successful]
    print(working_names)

# ============================================================
# MAIN
# ============================================================

async def main():
    providers = get_chat_providers()
    print(f"Found {len(providers)} chat-capable providers.")
    print(f"Running with {MAX_CONCURRENT} concurrent workers (timeout={TIMEOUT}s, retries={MAX_RETRIES})...\n")

    results = await run_all(providers)
    display_results(results)

if __name__ == "__main__":
    asyncio.run(main())