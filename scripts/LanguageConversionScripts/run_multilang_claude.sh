#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-output/sampled_200_distinct_tasks.jsonl}"

echo "[1/4] Tamil"
conda run -n spider2 python3 scripts/LanguageConversionScripts/translate_to_hindi.py \
  --input_file "$INPUT_FILE" \
  --output_file output/sampled_200_distinct_tasks_tamil_claude.jsonl \
  --target_language Tamil

echo "[2/4] Bengali"
conda run -n spider2 python3 scripts/LanguageConversionScripts/translate_to_hindi.py \
  --input_file "$INPUT_FILE" \
  --output_file output/sampled_200_distinct_tasks_bengali_claude.jsonl \
  --target_language Bengali

echo "[3/4] Marathi"
conda run -n spider2 python3 scripts/LanguageConversionScripts/translate_to_hindi.py \
  --input_file "$INPUT_FILE" \
  --output_file output/sampled_200_distinct_tasks_marathi_claude.jsonl \
  --target_language Marathi

echo "[4/4] Telugu"
conda run -n spider2 python3 scripts/LanguageConversionScripts/translate_to_hindi.py \
  --input_file "$INPUT_FILE" \
  --output_file output/sampled_200_distinct_tasks_telugu_claude.jsonl \
  --target_language Telugu

echo "All conversions completed."

