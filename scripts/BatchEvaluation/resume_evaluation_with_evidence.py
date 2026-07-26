#!/usr/bin/env python3
import os
import json
import subprocess
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
OUTPUT_DIR = PROJECT_ROOT / "output"
RUN_BULK_SCRIPT = PROJECT_ROOT / "scripts" / "BatchEvaluation" / "run_bulk_evaluation.py"

DATABASES = [
    "INDIA_Economic_Census_Firms",
    "INDIA_PRIMARY_POPULATION_CENSUS_1991"
]

LANGUAGES = ["english", "arabic", "spanish", "korean", "italian", "french", "chinese"]
NON_ENGLISH_SUFFIXES = ["arabic", "spanish", "korean", "italian", "french", "chinese", "hindi", "bengali", "tamil", "telugu", "marathi", "hinglish"]

def get_expected_count(db: str) -> int:
    if db == "INDIA_PRIMARY_POPULATION_CENSUS_1991":
        return 63
    return 100

def get_sampled_task_file(db: str, lang: str) -> Path:
    sampled_dir = OUTPUT_DIR / db / "sampled_tasks"
    if not sampled_dir.exists():
        return None
    pattern = f"*_{lang}.jsonl" if lang != "english" else "*.jsonl"
    files = list(sampled_dir.glob(pattern))
    if lang == "english":
        files = [f for f in files if not any(f"_{l}.jsonl" in f.name for l in NON_ENGLISH_SUFFIXES)]
    return files[0] if files else None

def identify_incomplete_runs() -> List[Tuple[str, str, Path, str]]:
    incomplete = []
    model_slug = "qwen_qwen3_8b"
    
    for db in DATABASES:
        expected = get_expected_count(db)
        eval_dir = OUTPUT_DIR / db / f"eval_files_oneshot_{model_slug}"
        
        for lang in LANGUAGES:
            sampled_file = get_sampled_task_file(db, lang)
            if not sampled_file:
                continue
            
            eval_pattern = f"*_{lang}_evaluated.jsonl" if lang != "english" else "*.jsonl"
            eval_files = list(eval_dir.glob(eval_pattern))
            if lang == "english":
                eval_files = [f for f in eval_files if not any(f"_{l}_evaluated.jsonl" in f.name for l in NON_ENGLISH_SUFFIXES)]
            
            # 1. Not Started
            if not eval_files:
                incomplete.append((db, lang, sampled_file, "Not Started (File Missing)"))
                continue
            
            eval_file = eval_files[0]
            try:
                with open(eval_file, "r", encoding="utf-8") as f:
                    lines = [line.strip() for line in f if line.strip()]
                
                rows = len(lines)
                
                # 2. Interrupted/Partial
                if rows < expected:
                    incomplete.append((db, lang, sampled_file, f"Incomplete ({rows}/{expected} completed)"))
                    continue
                    
                # 3. Connection Timeout/Operational Error Check ONLY
                # Standard SQL compile errors or standard query failures are normal and must NOT trigger a rerun.
                has_conn_error = False
                error_msg = ""
                for line in lines:
                    try:
                        data = json.loads(line)
                        err = data.get("error")
                        if err:
                            err_str = str(err).lower()
                            # Check specifically for network/DB server drops, NOT standard compile mismatches
                            if any(sub in err_str for sub in ["operationalerror", "connection timeout", "connection refused", "504 gateway"]):
                                has_conn_error = True
                                error_msg = str(err)
                                break
                    except Exception:
                        pass
                
                if has_conn_error:
                    incomplete.append((db, lang, sampled_file, f"DB Connection Lost: {error_msg[:60]}..."))
                    
            except Exception as e:
                incomplete.append((db, lang, sampled_file, f"Error Reading Evaluated File: {str(e)}"))
                
    return incomplete

def main():
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ Error: OPENROUTER_API_KEY environment variable is required.")
        return
        
    print("🔍 Auditing Qwen with-evidence runs under strictly precise criteria...")
    incomplete = identify_incomplete_runs()
    
    if not incomplete:
        print("🎉 All Qwen with-evidence runs are already 100% complete!")
        return
        
    print(f"📋 Found {len(incomplete)} incomplete Qwen runs:")
    for db, lang, file_path, reason in incomplete:
        print(f"  - {db} | {lang} | {reason} | Path: {file_path.name}")
        
    for db, lang, file_path, reason in incomplete:
        print(f"\n🚀 Running: {db} | {lang} ({reason})...")
        
        cmd = [
            "python3.10", str(RUN_BULK_SCRIPT),
            "--files", str(file_path),
            "--model", "qwen/qwen3-8b",
            "--workers", "6"
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd)

if __name__ == "__main__":
    main()
