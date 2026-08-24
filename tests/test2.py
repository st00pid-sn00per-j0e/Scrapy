import time
import g4f
from g4f import Provider
from g4f.client import Client


# ============================================================
# CONFIG
# ============================================================

Gemini = Provider.Gemini
client = Client()

TEST_PROMPT = "Reply with exactly: MODEL_TEST_SUCCESS"


# ============================================================
# UI
# ============================================================

SEPARATOR = "=" * 90
SUB = "-" * 90


# ============================================================
# TEST ALL MODELS
# ============================================================

def test_models():

    models = list(getattr(Gemini, "models", []))

    print(SEPARATOR)
    print("                 G4F GEMINI MODEL TEST")
    print(SEPARATOR)

    print(f"Provider : {Gemini}")
    print(f"Models   : {len(models)}")

    print("\nAvailable models:")

    for i, model in enumerate(models, 1):
        print(f"{i:2}. {model}")

    print("\n" + SEPARATOR)
    print("TESTING ALL GEMINI MODELS")
    print(SEPARATOR)

    working = []
    failed = []

    for index, model in enumerate(models, 1):

        print(f"\n[{index}/{len(models)}] MODEL: {model}")
        print(SUB)

        start = time.time()
        first_token_time = None
        full_response = ""

        print("AI > ", end="", flush=True)

        try:

            response = client.chat.completions.create(
                model=model,
                provider=Gemini,
                messages=[
                    {
                        "role": "user",
                        "content": TEST_PROMPT
                    }
                ],
                stream=True,
            )

            for chunk in response:

                if first_token_time is None:
                    first_token_time = time.time()

                try:
                    content = chunk.choices[0].delta.content
                except (AttributeError, IndexError):
                    content = None

                if content:
                    print(content, end="", flush=True)
                    full_response += content

            elapsed = time.time() - start

            print("\n")

            if full_response.strip():

                working.append({
                    "model": model,
                    "time": elapsed,
                    "ttft": (
                        first_token_time - start
                        if first_token_time
                        else None
                    )
                })

                print("STATUS: SUCCESS")
                print(f"TTFT   : {first_token_time - start:.2f}s"
                      if first_token_time else "TTFT   : N/A")
                print(f"TIME   : {elapsed:.2f}s")

            else:

                failed.append({
                    "model": model,
                    "error": "Empty response"
                })

                print("STATUS: FAILED")
                print("ERROR  : Empty response")

        except Exception as e:

            elapsed = time.time() - start

            failed.append({
                "model": model,
                "error": str(e)
            })

            print("\n")
            print("STATUS    : FAILED")
            print("ERROR TYPE:", type(e).__name__)
            print("ERROR     :", str(e))
            print(f"TIME      : {elapsed:.2f}s")

    return working, failed


# ============================================================
# SHOW RESULTS
# ============================================================

def show_results(working, failed):

    print("\n" + SEPARATOR)
    print("FINAL GEMINI MODEL AVAILABILITY")
    print(SEPARATOR)

    print(f"\nTotal tested : {len(working) + len(failed)}")
    print(f"Working      : {len(working)}")
    print(f"Failed       : {len(failed)}")

    print("\n" + SEPARATOR)
    print("WORKING / STREAMING MODELS")
    print(SEPARATOR)

    for i, item in enumerate(working, 1):

        print(
            f"{i:2}. "
            f"{item['model']:<40} "
            f"{item['time']:.2f}s"
        )

    if failed:

        print("\n" + SEPARATOR)
        print("FAILED MODELS")
        print(SEPARATOR)

        for i, item in enumerate(failed, 1):

            print(f"{i:2}. {item['model']}")
            print(f"    {item['error']}")

    print("\n" + SEPARATOR)
    print("WORKING MODELS - PYTHON LIST")
    print(SEPARATOR)

    print("[")

    for item in working:
        print(f'    "{item["model"]}",')

    print("]")


# ============================================================
# MODEL SELECTION
# ============================================================

def select_model(working):

    if not working:
        print("\nNo working models available.")
        return None

    print("\n\n" + SEPARATOR)
    print("SELECT GEMINI MODEL FOR LIVE CHAT")
    print(SEPARATOR)

    for i, item in enumerate(working, 1):

        print(
            f"{i:2}. "
            f"{item['model']:<40} "
            f"{item['time']:.2f}s"
        )

    print("\nEnter the number of the model.")
    print("Enter 'q' to quit.")

    while True:

        selection = input("\nModel > ").strip()

        if selection.lower() == "q":
            return None

        try:

            number = int(selection)

            if 1 <= number <= len(working):

                return working[number - 1]["model"]

            print("Invalid model number.")

        except ValueError:

            print("Enter a numeric model number.")


# ============================================================
# LIVE CHAT
# ============================================================

def live_chat(model):

    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful AI assistant. "
                "Give accurate and useful answers."
            )
        }
    ]

    print("\n\n" + SEPARATOR)
    print("                 GEMINI LIVE CLI CHAT")
    print(SEPARATOR)

    print(f"Model    : {model}")
    print("Provider : G4F / Gemini")

    print("\nCommands:")
    print("  /exit       Exit chat")
    print("  /clear      Clear conversation")
    print("  /history    Show conversation")
    print("  /model      Show current model")
    print("  /new        Start new conversation")

    print(SEPARATOR)

    while True:

        try:

            user_input = input("\nYou > ").strip()

        except KeyboardInterrupt:

            print("\n\nExiting...")
            break

        except EOFError:

            print("\n\nExiting...")
            break

        if not user_input:
            continue

        # ----------------------------------------------------
        # EXIT
        # ----------------------------------------------------

        if user_input.lower() in (
            "/exit",
            "/quit",
            "/q"
        ):

            print("\nExiting...")
            break

        # ----------------------------------------------------
        # MODEL
        # ----------------------------------------------------

        if user_input.lower() == "/model":

            print()
            print(f"Model    : {model}")
            print("Provider : G4F / Gemini")
            print("Streaming: enabled")
            continue

        # ----------------------------------------------------
        # CLEAR
        # ----------------------------------------------------

        if user_input.lower() in (
            "/clear",
            "/new"
        ):

            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a helpful AI assistant. "
                        "Give accurate and useful answers."
                    )
                }
            ]

            print("\n[Conversation cleared]")
            continue

        # ----------------------------------------------------
        # HISTORY
        # ----------------------------------------------------

        if user_input.lower() == "/history":

            print("\n" + SUB)
            print("CONVERSATION HISTORY")
            print(SUB)

            for message in messages:

                role = message["role"].upper()

                print(f"\n{role}:")
                print(message["content"])

            continue

        # ----------------------------------------------------
        # USER MESSAGE
        # ----------------------------------------------------

        messages.append(
            {
                "role": "user",
                "content": user_input
            }
        )

        print("\nAI > ", end="", flush=True)

        start = time.time()
        first_token = None
        full_response = ""

        try:

            response = client.chat.completions.create(
                model=model,
                provider=Gemini,
                messages=messages,
                stream=True,
            )

            for chunk in response:

                if first_token is None:
                    first_token = time.time()

                try:
                    content = chunk.choices[0].delta.content
                except (AttributeError, IndexError):
                    content = None

                if content:

                    print(
                        content,
                        end="",
                        flush=True
                    )

                    full_response += content

            elapsed = time.time() - start

            print()

            # ------------------------------------------------
            # STORE ASSISTANT RESPONSE
            # ------------------------------------------------

            if full_response:

                messages.append(
                    {
                        "role": "assistant",
                        "content": full_response
                    }
                )

            else:

                messages.pop()

                print("[Empty response]")

            # ------------------------------------------------
            # PERFORMANCE
            # ------------------------------------------------

            if first_token:

                ttft = first_token - start

                print(
                    f"\n[TTFT: {ttft:.2f}s | "
                    f"Total: {elapsed:.2f}s]"
                )

        except Exception as e:

            messages.pop()

            print("\n")
            print("[ERROR]")
            print("Type :", type(e).__name__)
            print("Error:", str(e))


# ============================================================
# MAIN
# ============================================================

def main():

    # 1. Test models
    working, failed = test_models()

    # 2. Show availability
    show_results(working, failed)

    # 3. Select working model
    model = select_model(working)

    if model is None:
        print("\nNo model selected.")
        return

    # 4. Start CLI
    live_chat(model)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()