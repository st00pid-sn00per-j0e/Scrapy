# test3.py

import time
import inspect
import importlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO


# ============================================================
# CONFIG
# ============================================================

TARGETS = [
    "AnyProvider",
    "AsyncGeneratorProvider",
    "AsyncProvider",
]

PROMPT = "Reply with exactly: PROVIDER_TEST_OK"

MAX_WORKERS = 12
TEST_TIMEOUT = 20

# Prevent noisy provider output from destroying the CLI.
SILENCE_PROVIDER_OUTPUT = True


# ============================================================
# IMPORT
# ============================================================

def silent_import(name):
    try:
        if SILENCE_PROVIDER_OUTPUT:
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                return importlib.import_module(name)

        return importlib.import_module(name)

    except Exception:
        return None


# ============================================================
# PROVIDER DISCOVERY
# ============================================================

def get_provider_class(name):

    modules = [
        "g4f.Provider",
        "g4f.Provider.base_provider",
        "g4f.Provider.any_provider",
        "g4f.Provider",
    ]

    for module_name in modules:

        module = silent_import(module_name)

        if module is None:
            continue

        try:
            obj = getattr(module, name, None)

            if inspect.isclass(obj):
                return obj

        except Exception:
            pass

    return None


# ============================================================
# MODEL DISCOVERY
# ============================================================

def normalize_models(value):

    models = set()

    if isinstance(value, str):
        models.add(value)

    elif isinstance(value, (list, tuple, set, frozenset)):

        for item in value:

            if isinstance(item, str):
                models.add(item)

            elif isinstance(item, dict):
                for k, v in item.items():

                    if isinstance(k, str):
                        models.add(k)

                    if isinstance(v, str):
                        models.add(v)

    elif isinstance(value, dict):

        for k, v in value.items():

            if isinstance(k, str):
                models.add(k)

            if isinstance(v, str):
                models.add(v)

            elif isinstance(v, (list, tuple, set)):

                for item in v:

                    if isinstance(item, str):
                        models.add(item)

    return models


def get_models(provider):

    models = set()

    attributes = [
        "models",
        "model",
        "default_model",
        "model_list",
        "supported_models",
        "MODEL",
        "MODELS",
    ]

    for attr in attributes:

        try:

            value = getattr(provider, attr, None)

            if value is not None:
                models.update(normalize_models(value))

        except Exception:
            pass

    return sorted(models)


# ============================================================
# RESPONSE EXTRACTION
# ============================================================

def extract_response(response):

    text = ""

    # Streaming generator
    if inspect.isgenerator(response):

        for chunk in response:

            try:

                content = chunk.choices[0].delta.content

                if content:
                    text += content

            except Exception:
                pass

        return text.strip()

    # Async generator accidentally returned
    if inspect.isasyncgen(response):
        return "ASYNC_GENERATOR_RETURNED"

    # Normal response
    try:

        content = response.choices[0].message.content

        if content:
            return str(content).strip()

    except Exception:
        pass

    # Fallback
    try:
        return str(response).strip()
    except Exception:
        return ""


# ============================================================
# SINGLE MODEL TEST
# ============================================================

def test_model(provider_name, provider, model):

    start = time.perf_counter()

    try:

        client_module = silent_import("g4f.client")

        if client_module is None:
            return {
                "success": False,
                "provider": provider_name,
                "model": model,
                "elapsed": 0,
                "error": "CLIENT_IMPORT_FAILED",
            }

        Client = getattr(client_module, "Client")

        client = Client()

        messages = [
            {
                "role": "user",
                "content": PROMPT,
            }
        ]

        # IMPORTANT:
        # stdout/stderr suppression is local to this worker only.
        if SILENCE_PROVIDER_OUTPUT:

            with redirect_stdout(StringIO()), \
                 redirect_stderr(StringIO()):

                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    provider=provider,
                )

                text = extract_response(response)

        else:

            response = client.chat.completions.create(
                model=model,
                messages=messages,
                provider=provider,
            )

            text = extract_response(response)

        elapsed = time.perf_counter() - start

        if text:

            return {
                "success": True,
                "provider": provider_name,
                "model": model,
                "elapsed": elapsed,
                "response": text,
            }

        return {
            "success": False,
            "provider": provider_name,
            "model": model,
            "elapsed": elapsed,
            "error": "EMPTY_RESPONSE",
        }

    except Exception as e:

        elapsed = time.perf_counter() - start

        return {
            "success": False,
            "provider": provider_name,
            "model": model,
            "elapsed": elapsed,
            "error": f"{type(e).__name__}: {str(e)[:180]}",
        }


# ============================================================
# TIMEOUT WRAPPER
# ============================================================

def timed_test(provider_name, provider, model):

    result = {}

    finished = threading.Event()

    def worker():

        nonlocal result

        try:
            result = test_model(
                provider_name,
                provider,
                model,
            )

        except Exception as e:

            result = {
                "success": False,
                "provider": provider_name,
                "model": model,
                "elapsed": 0,
                "error": f"{type(e).__name__}: {e}",
            }

        finally:
            finished.set()

    thread = threading.Thread(
        target=worker,
        daemon=True,
    )

    thread.start()

    if not finished.wait(TEST_TIMEOUT):

        return {
            "success": False,
            "provider": provider_name,
            "model": model,
            "elapsed": TEST_TIMEOUT,
            "error": f"TIMEOUT>{TEST_TIMEOUT}s",
        }

    return result


# ============================================================
# DISCOVERY
# ============================================================

def discover_matrix():

    matrix = []

    print()
    print("Discovering provider implementations...")

    for provider_name in TARGETS:

        provider = get_provider_class(provider_name)

        if provider is None:

            print(
                f"  ✗ {provider_name:<30} "
                f"class not found"
            )

            continue

        models = get_models(provider)

        print(
            f"  ✓ {provider_name:<30} "
            f"{len(models)} models"
        )

        for model in models:

            matrix.append(
                (
                    provider_name,
                    provider,
                    model,
                )
            )

    return matrix


# ============================================================
# MAIN PARALLEL TEST
# ============================================================

def main():

    print("=" * 110)
    print(
        "                         NIZAMI PROVIDER MATRIX TEST"
    )
    print("=" * 110)

    matrix = discover_matrix()

    print()
    print(
        f"Combinations discovered : {len(matrix)}"
    )

    print(
        f"Parallel workers        : {MAX_WORKERS}"
    )

    print(
        f"Per-test timeout        : {TEST_TIMEOUT}s"
    )

    print()
    print("-" * 110)

    if not matrix:

        print("No provider/model combinations discovered.")
        return

    results = []

    completed = 0
    total = len(matrix)

    start_all = time.perf_counter()

    # --------------------------------------------------------
    # PARALLEL EXECUTION
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {}

        for provider_name, provider, model in matrix:

            future = executor.submit(
                timed_test,
                provider_name,
                provider,
                model,
            )

            futures[future] = (
                provider_name,
                model,
            )

        for future in as_completed(futures):

            provider_name, model = futures[future]

            completed += 1

            try:
                result = future.result()

            except Exception as e:

                result = {
                    "success": False,
                    "provider": provider_name,
                    "model": model,
                    "elapsed": 0,
                    "error": f"{type(e).__name__}: {e}",
                }

            results.append(result)

            if result["success"]:

                response = result.get(
                    "response",
                    "",
                )

                response = response.replace(
                    "\n",
                    " ",
                )[:80]

                status = (
                    f"✓ {result['elapsed']:.2f}s "
                    f"| {response}"
                )

            else:

                status = (
                    f"✗ {result.get('error', 'FAILED')}"
                )

            print(
                f"[{completed:>4}/{total}] "
                f"{provider_name:<28} "
                f"{model:<40} "
                f"{status}",
                flush=True,
            )

    total_time = time.perf_counter() - start_all

    # ========================================================
    # RESULTS
    # ========================================================

    working = [
        r for r in results
        if r["success"]
    ]

    failed = [
        r for r in results
        if not r["success"]
    ]

    print()
    print("=" * 110)
    print(
        "                              FINAL RESULTS"
    )
    print("=" * 110)

    print()
    print(
        f"Total combinations : {len(results)}"
    )

    print(
        f"Working            : {len(working)}"
    )

    print(
        f"Failed             : {len(failed)}"
    )

    print(
        f"Wall-clock time    : {total_time:.2f}s"
    )

    # ========================================================
    # WORKING
    # ========================================================

    print()
    print("=" * 110)
    print("                              WORKING")
    print("=" * 110)

    for r in sorted(
        working,
        key=lambda x: x["elapsed"],
    ):

        response = r.get(
            "response",
            "",
        ).replace(
            "\n",
            " ",
        )[:100]

        print(
            f"✓ {r['provider']:<28} "
            f"{r['model']:<40} "
            f"{r['elapsed']:>7.2f}s "
            f"| {response}"
        )

    # ========================================================
    # FAILED
    # ========================================================

    print()
    print("=" * 110)
    print("                              FAILED")
    print("=" * 110)

    for r in failed:

        print(
            f"✗ {r['provider']:<28} "
            f"{r['model']:<40} "
            f"| {r.get('error')}"
        )

    # ========================================================
    # PROVIDER SUMMARY
    # ========================================================

    print()
    print("=" * 110)
    print("                           PROVIDER SUMMARY")
    print("=" * 110)

    for provider_name in TARGETS:

        provider_results = [
            r for r in results
            if r["provider"] == provider_name
        ]

        provider_working = [
            r for r in provider_results
            if r["success"]
        ]

        print()
        print(
            f"{provider_name}"
        )

        print(
            f"  Tested : {len(provider_results)}"
        )

        print(
            f"  Passed : {len(provider_working)}"
        )

        print(
            f"  Failed : "
            f"{len(provider_results) - len(provider_working)}"
        )

    print()
    print("=" * 110)
    print("TEST COMPLETE")
    print("=" * 110)


if __name__ == "__main__":
    main()