#!/usr/bin/env python3
"""
Refactored translator for NLQ-SQL task JSONL.

Translates only the `question` field and preserves all other fields.
Despite the filename, this supports any target language.
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, Any

from anthropic import Anthropic
from tqdm import tqdm

DEFAULT_FOUR_LANGUAGES = ["Tamil", "Bengali", "Marathi", "Telugu"]


def translate_question(
    client: Anthropic,
    question_text: str,
    target_language: str,
    model: str,
    max_retries: int = 3,
) -> str:
    """Translate a single question to target language with retry fallback."""
    system_prompt = (
        f"You are a professional translator. Translate the user query into {target_language}. "
        f"Return only the {target_language} translation with no explanations."
    )
    user_prompt = f"Translate this query:\n{question_text}"

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=1000,
                temperature=0.2,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            translated = response.content[0].text.strip()
            if translated:
                return translated
            raise RuntimeError("Empty translation response")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"[WARN] Translation failed after retries: {e}")
                return question_text

    return question_text


def translate_jsonl_questions(
    input_file: str,
    output_file: str,
    target_language: str,
    model: str,
    start_index: int = 0,
    end_index: int = None,
    sleep_seconds: float = 0.2,
) -> None:
    """Translate `question` field in JSONL records and write output JSONL."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY is not set")
        sys.exit(1)

    client = Anthropic(api_key=api_key)

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if end_index is None:
        end_index = len(lines)
    lines_to_process = lines[start_index:end_index]

    print(f"Total records: {len(lines)}")
    print(f"Processing range: [{start_index}, {end_index}) -> {len(lines_to_process)} records")
    print(f"Target language: {target_language}")
    print(f"Model: {model}")

    translated_records = []
    failed = 0

    for idx, line in enumerate(tqdm(lines_to_process, desc="Translating questions")):
        absolute_idx = start_index + idx
        try:
            obj: Dict[str, Any] = json.loads(line.strip())
            question = (obj.get("question") or "").strip()
            if not question:
                translated_records.append(obj)
                continue

            translated_q = translate_question(client, question, target_language, model)
            obj["question"] = translated_q
            translated_records.append(obj)

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as e:
            failed += 1
            print(f"[WARN] Failed record {absolute_idx}: {e}")
            try:
                translated_records.append(json.loads(line.strip()))
            except Exception:
                continue

    with open(output_file, "w", encoding="utf-8") as f:
        for rec in translated_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("Done.")
    print(f"Output: {output_file}")
    print(f"Processed: {len(lines_to_process)} | Failed: {failed}")


def main():
    parser = argparse.ArgumentParser(description="Translate NLQ-SQL task questions to target language")
    parser.add_argument("--input_file", required=True, help="Input JSONL path")
    parser.add_argument("--output_file", default=None, help="Output JSONL path")
    parser.add_argument("--target_language", default="Hindi", help="Target language name (e.g., Hindi, Bengali)")
    parser.add_argument(
        "--target_languages",
        default=None,
        help="Comma-separated target languages for multi-run (e.g., 'Tamil,Bengali,Marathi,Telugu')",
    )
    parser.add_argument(
        "--all_four",
        action="store_true",
        help="Translate into Tamil, Bengali, Marathi, and Telugu in one run",
    )
    parser.add_argument("--model", default="claude-haiku-4-5", help="Anthropic model")
    parser.add_argument("--start_index", type=int, default=0, help="Start index (inclusive)")
    parser.add_argument("--end_index", type=int, default=None, help="End index (exclusive)")
    parser.add_argument("--sleep_seconds", type=float, default=0.2, help="Delay between requests")
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)

    if args.all_four:
        target_languages = DEFAULT_FOUR_LANGUAGES
    elif args.target_languages:
        target_languages = [x.strip() for x in args.target_languages.split(",") if x.strip()]
        if not target_languages:
            print("Error: --target_languages provided but empty after parsing")
            sys.exit(1)
    else:
        target_languages = [args.target_language]

    for lang in target_languages:
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

        print(f"\n=== Translating to {lang} ===")
        started = time.time()
        translate_jsonl_questions(
            input_file=args.input_file,
            output_file=output_file,
            target_language=lang,
            model=args.model,
            start_index=args.start_index,
            end_index=args.end_index,
            sleep_seconds=args.sleep_seconds,
        )
        elapsed = time.time() - started
        print(f"=== Completed {lang} in {elapsed:.1f} seconds ===")


if __name__ == "__main__":
    main()
