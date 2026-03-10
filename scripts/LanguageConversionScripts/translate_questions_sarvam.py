#!/usr/bin/env python3
"""
Translate the `question` field in a JSONL NLQ-SQL task file using Sarvam Translate API.

Expected input JSONL record shape:
{
  "question": "...",
  "sql": "...",
  ...
}
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, Any

import requests
from tqdm import tqdm


SARVAM_TRANSLATE_URL = "https://api.sarvam.ai/translate"


def translate_text_sarvam(
    text: str,
    api_key: str,
    target_language_code: str,
    source_language_code: str = "auto",
    timeout: int = 60,
    max_retries: int = 3,
) -> str:
    """Translate one text string via Sarvam API. Returns original text on repeated failure."""
    headers = {
        "api-subscription-key": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "input": text,
        "source_language_code": source_language_code,
        "target_language_code": target_language_code,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                SARVAM_TRANSLATE_URL,
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")

            data = resp.json()
            # Handle common response keys defensively.
            translated = (
                data.get("translated_text")
                or data.get("translation")
                or data.get("output")
                or data.get("result")
            )
            if isinstance(translated, str) and translated.strip():
                return translated.strip()

            raise RuntimeError(f"Unexpected response payload: {data}")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"[WARN] Translation failed after {max_retries} attempts: {e}")
                return text

    return text


def process_jsonl(
    input_file: str,
    output_file: str,
    api_key: str,
    target_language_code: str,
    source_language_code: str = "auto",
    start_index: int = 0,
    end_index: int = None,
    sleep_seconds: float = 0.1,
) -> None:
    """Translate `question` field in selected JSONL range and write translated JSONL."""
    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if end_index is None:
        end_index = len(lines)

    if start_index < 0 or end_index < start_index:
        raise ValueError("Invalid start/end index")

    lines_to_process = lines[start_index:end_index]
    translated_records = []
    failures = 0

    print(f"Total records: {len(lines)}")
    print(f"Processing range: [{start_index}, {end_index}) -> {len(lines_to_process)} records")
    print(f"Target language code: {target_language_code}")

    for i, line in enumerate(tqdm(lines_to_process, desc="Translating questions")):
        absolute_idx = start_index + i
        try:
            obj: Dict[str, Any] = json.loads(line)
            question = (obj.get("question") or "").strip()
            if not question:
                translated_records.append(obj)
                continue

            translated_q = translate_text_sarvam(
                text=question,
                api_key=api_key,
                target_language_code=target_language_code,
                source_language_code=source_language_code,
            )
            obj["question"] = translated_q
            translated_records.append(obj)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as e:
            failures += 1
            print(f"[WARN] Record {absolute_idx} failed: {e}")
            try:
                translated_records.append(json.loads(line))
            except Exception:
                continue

    with open(output_file, "w", encoding="utf-8") as f:
        for rec in translated_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("Done.")
    print(f"Output: {output_file}")
    print(f"Processed: {len(lines_to_process)} | Failures: {failures}")


def main():
    parser = argparse.ArgumentParser(description="Translate JSONL task questions using Sarvam API")
    parser.add_argument("--input_file", required=True, help="Input JSONL path")
    parser.add_argument("--output_file", required=True, help="Output JSONL path")
    parser.add_argument("--target_language_code", required=True, help="Target language code (e.g., hi-IN, bn-IN)")
    parser.add_argument("--source_language_code", default="auto", help="Source language code (default: auto)")
    parser.add_argument(
        "--api_key",
        default=None,
        help="Sarvam API subscription key. Defaults to SARVAM_API_KEY env var.",
    )
    parser.add_argument("--start_index", type=int, default=0, help="Start index (inclusive)")
    parser.add_argument("--end_index", type=int, default=None, help="End index (exclusive)")
    parser.add_argument("--sleep_seconds", type=float, default=0.1, help="Delay between API calls")

    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("SARVAM_API_KEY")
    if not api_key:
        print("Error: Provide --api_key or set SARVAM_API_KEY")
        sys.exit(1)

    process_jsonl(
        input_file=args.input_file,
        output_file=args.output_file,
        api_key=api_key,
        target_language_code=args.target_language_code,
        source_language_code=args.source_language_code,
        start_index=args.start_index,
        end_index=args.end_index,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()

