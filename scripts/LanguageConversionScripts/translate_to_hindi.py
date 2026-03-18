import argparse
import json
import os
import sys
import time
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

DEFAULT_SIX_LANGUAGES = ["Tamil", "Bengali", "Marathi", "Telugu", "Hindi", "Hinglish"]


# ---------------------------------------------------------------------------
# LLM Backends
# ---------------------------------------------------------------------------

class AnthropicBackend:
    """Direct Anthropic API backend."""

    def __init__(self, api_key: str, model: str):
        from anthropic import Anthropic
        self.client = Anthropic(api_key=api_key)
        self.model = model

    def translate(self, system_prompt: str, user_prompt: str) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1000,
            temperature=0.2,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text.strip()


class OpenRouterBackend:
    """OpenRouter API backend (works with any model on OpenRouter)."""

    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"

    def translate(self, system_prompt: str, user_prompt: str) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 1000,
        }
        response = requests.post(
            self.base_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=(15, 120),
        )
        if response.status_code != 200:
            raise RuntimeError(f"OpenRouter API error: {response.status_code} - {response.text}")
        return response.json()["choices"][0]["message"]["content"].strip()


def create_backend(provider: str, model: str, api_key: Optional[str] = None):
    """Factory: create the right backend based on --provider flag."""
    if provider == "openrouter":
        key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not key:
            print("Error: OPENROUTER_API_KEY not found. Set via --api-key or environment variable.")
            sys.exit(1)
        return OpenRouterBackend(api_key=key, model=model)
    else:
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            print("Error: ANTHROPIC_API_KEY not found. Set via --api-key or environment variable.")
            sys.exit(1)
        return AnthropicBackend(api_key=key, model=model)


# ---------------------------------------------------------------------------
# Translation logic
# ---------------------------------------------------------------------------

def translate_question(
    backend,
    question_text: str,
    target_language: str,
    max_retries: int = 3,
) -> str:
    """Translate a single question to target language with retry fallback."""
    is_hinglish = target_language.strip().lower() == "hinglish"
    if is_hinglish:
        system_prompt = (
            "You are a professional translator for Text-to-SQL data generation. "
            "Return only the translated text with no explanations."
        )
        user_prompt = (
            "Translate this English Text-to-SQL prompt into natural Hinglish using Roman script. "
            "Keep all table names, column names, and SQL-specific values in their original English. "
            "Only translate the natural language intent and the conversational structure. "
            "Keep it technical but fluid.\n\n"
            f"Text:\n{question_text}"
        )
    else:
        system_prompt = (
            f"You are a professional translator. Translate the user query into {target_language}. "
            f"Return only the {target_language} translation with no explanations."
        )
        user_prompt = f"Translate this query:\n{question_text}"

    for attempt in range(max_retries):
        try:
            translated = backend.translate(system_prompt, user_prompt)
            if translated:
                return translated
            raise RuntimeError("Empty translation response")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"[WARN] Translation failed after retries for '{question_text[:30]}...': {e}")
                return question_text

    return question_text


def translate_jsonl_questions(
    input_file: str,
    output_file: str,
    target_language: str,
    backend,
    start_index: int = 0,
    end_index: int = None,
    sleep_seconds: float = 0.0,
    max_workers: int = 5,
) -> None:
    """Translate `question` field in JSONL records and write output JSONL in parallel."""
    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if end_index is None:
        end_index = len(lines)
    lines_to_process = lines[start_index:end_index]

    print(f"Total records: {len(lines)}")
    print(f"Processing range: [{start_index}, {end_index}) -> {len(lines_to_process)} records")
    print(f"Target language: {target_language}")
    print(f"Parallel workers: {max_workers}")

    # Prepare indices and objects
    objs = []
    for line in lines_to_process:
        try:
            objs.append(json.loads(line.strip()))
        except Exception as e:
            print(f"[WARN] Failed to parse line: {e}")
            objs.append(None)

    def process_record(idx, obj):
        if obj is None:
            return idx, None
        
        question = (obj.get("question") or "").strip()
        if not question:
            return idx, obj
        
        try:
            translated_q = translate_question(backend, question, target_language)
            obj_copy = dict(obj)
            obj_copy["question"] = translated_q
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            return idx, obj_copy
        except Exception as e:
            print(f"[WARN] Error translating record {idx}: {e}")
            return idx, obj

    # Execute in parallel
    results = [None] * len(objs)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(process_record, i, obj): i 
            for i, obj in enumerate(objs)
        }
        
        for future in tqdm(as_completed(future_to_idx), total=len(objs), desc=f"Translating {target_language}"):
            res = future.result()
            if res:
                idx, result_obj = res
                results[idx] = result_obj

    # Filter out None results if any parse errors occurred
    final_records = [r for r in results if r is not None]

    with open(output_file, "w", encoding="utf-8") as f:
        for rec in final_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Completed {target_language}. Output: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Parallel translate NLQ-SQL task questions")
    parser.add_argument("--input_file", required=True, help="Input JSONL path")
    parser.add_argument("--output_file", default=None, help="Output JSONL path")
    parser.add_argument("--target_language", default="Hindi", help="Target language name")
    parser.add_argument(
        "--target_languages",
        default=None,
        help="Comma-separated target languages (e.g., 'Tamil,Bengali,Marathi')",
    )
    parser.add_argument(
        "--all_four",
        action="store_true",
        help="Translate into 6 languages",
    )
    parser.add_argument(
        "--all_six",
        action="store_true",
        help="Translate into 6 languages",
    )
    parser.add_argument("--model", default="claude-3-5-haiku-20241022", help="Model name")
    parser.add_argument(
        "--provider",
        default="anthropic",
        choices=["anthropic", "openrouter"],
        help="API provider: 'anthropic' or 'openrouter'",
    )
    parser.add_argument("--api-key", default=None, help="API key")
    parser.add_argument("--start_index", type=int, default=0, help="Start index")
    parser.add_argument("--end_index", type=int, default=None, help="End index")
    parser.add_argument("--sleep_seconds", type=float, default=0.0, help="Delay between requests per worker")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel translation workers per language")
    parser.add_argument("--lang_workers", type=int, default=1, help="Number of languages to process in parallel")
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)

    # Determine target languages
    if args.all_four or args.all_six:
        target_languages = DEFAULT_SIX_LANGUAGES
    elif args.target_languages:
        target_languages = [x.strip() for x in args.target_languages.split(",") if x.strip()]
    else:
        target_languages = [args.target_language]

    print(f"Using provider: {args.provider} | Model: {args.model}")
    print(f"Processing {len(target_languages)} languages: {target_languages}")

    def run_translation_for_lang(lang):
        # Create a fresh backend per thread/process to avoid shared state issues if any
        backend = create_backend(args.provider, args.model, args.api_key)
        
        output_file = args.output_file
        if output_file is None:
            base, ext = os.path.splitext(args.input_file)
            suffix = lang.lower().replace(" ", "_")
            output_file = f"{base}_{suffix}{ext or '.jsonl'}"
        elif len(target_languages) > 1:
            base, ext = os.path.splitext(output_file)
            suffix = lang.lower().replace(" ", "_")
            output_file = f"{base}_{suffix}{ext or '.jsonl'}"

        out_dir = os.path.dirname(output_file)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        started = time.time()
        translate_jsonl_questions(
            input_file=args.input_file,
            output_file=output_file,
            target_language=lang,
            backend=backend,
            start_index=args.start_index,
            end_index=args.end_index,
            sleep_seconds=args.sleep_seconds,
            max_workers=args.workers,
        )
        elapsed = time.time() - started
        print(f"=== Completed {lang} in {elapsed:.1f} seconds ===")

    if args.lang_workers > 1 and len(target_languages) > 1:
        print(f"Running across {args.lang_workers} languages in parallel...")
        with ThreadPoolExecutor(max_workers=args.lang_workers) as lang_executor:
            lang_executor.map(run_translation_for_lang, target_languages)
    else:
        for lang in target_languages:
            run_translation_for_lang(lang)


if __name__ == "__main__":
    main()
