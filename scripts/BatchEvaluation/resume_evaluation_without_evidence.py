#!/usr/bin/env python3
import os
import json
import subprocess
from pathlib import Path
from typing import List, Dict, Tuple

PROJECT_ROOT = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
OUTPUT_DIR = PROJECT_ROOT / "output"
RUN_BULK_SCRIPT = PROJECT_ROOT / "scripts" / "BatchEvaluation" / "run_bulk_evaluation.py"

DATABASES = [
    "INDIA_Economic_Census_Firms",
    "INDIA_ICRISAT_District_Level_Agricultural_Data",
    "INDIA_PRIMARY_POPULATION_CENSUS_1991"
]

LANGUAGES = ["english", "arabic", "spanish", "korean", "italian", "french", "chinese"]

MODELS = [
    "deepseek/deepseek-v3.2",
    "meta-llama/llama-3.3-70b-instruct",
    "minimax/minimax-m2.7",
    "qwen/qwen3-8b"
]

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
        # Filter out foreign and Indic languages
        files = [
            f for f in files 
            if not any(f"_{l}.jsonl" in f.name for l in NON_ENGLISH_SUFFIXES)
        ]
        
    return files[0] if files else None

def identify_incomplete_runs() -> Dict[str, List[Tuple[str, str, Path]]]:
    """
    Identifies all model-db-language runs that are:
    - Not started (evaluated file doesn't exist)
    - Interrupted (evaluated file has fewer lines than expected)
    - Failed/DB Timeout (evaluated file contains Postgres or API errors in lines)
    
    Returns a dict mapping: model -> list of (db_name, lang, sampled_task_file)
    """
    incomplete = {model: [] for model in MODELS}
    
    for model in MODELS:
        model_slug = model.replace("/", "_").replace("-", "_").replace(".", "_")
        for db in DATABASES:
            expected = get_expected_count(db)
            eval_dir = OUTPUT_DIR / db / f"eval_files_oneshot_{model_slug}_no_evidence"
            
            for lang in LANGUAGES:
                sampled_file = get_sampled_task_file(db, lang)
                if not sampled_file:
                    print(f"⚠️ Warning: Sampled task file not found for {db} ({lang})")
                    continue
                
                # Check evaluated file
                eval_pattern = f"*_{lang}_evaluated.jsonl" if lang != "english" else "*.jsonl"
                eval_files = list(eval_dir.glob(eval_pattern))
                if lang == "english":
                    eval_files = [
                        f for f in eval_files 
                        if not any(f"_{l}_evaluated.jsonl" in f.name for l in NON_ENGLISH_SUFFIXES)
                    ]
                
                # 1. Not Started
                if not eval_files:
                    incomplete[model].append((db, lang, sampled_file, "Not Started (File Missing)"))
                    continue
                
                eval_file = eval_files[0]
                try:
                    with open(eval_file, "r", encoding="utf-8") as f:
                        lines = [line.strip() for line in f if line.strip()]
                    
                    rows = len(lines)
                    
                    # 2. Interrupted/Partial
                    if rows < expected:
                        incomplete[model].append((db, lang, sampled_file, f"Interrupted ({rows}/{expected} completed)"))
                        continue
                        
                    # 3. DB Error/Timeout Check
                    has_error = False
                    error_msg = ""
                    for line in lines:
                        try:
                            data = json.loads(line)
                            err = data.get("error")
                            if err and any(sub in str(err).lower() for sub in ["connection", "timeout", "operationalerror", "failed"]):
                                has_error = True
                                error_msg = str(err)
                                break
                        except Exception:
                            pass
                    
                    if has_error:
                        incomplete[model].append((db, lang, sampled_file, f"DB Error/Timeout: {error_msg[:60]}..."))
                        
                except Exception as e:
                    incomplete[model].append((db, lang, sampled_file, f"Error Reading Evaluated File: {str(e)}"))
                    
    return incomplete

def main():
    api_key_set = os.environ.get("OPENROUTER_API_KEY") is not None
        
    print("==================================================")
    print("🔍 Auditing Bulk Evaluation Files for Resumption...")
    print("==================================================")
    
    incomplete_runs = identify_incomplete_runs()
    
    total_incomplete = sum(len(runs) for runs in incomplete_runs.values())
    if total_incomplete == 0:
        print("🎉 All 84 evaluations are successfully completed with no errors! Nothing to do.")
        return
        
    print(f"\nFound {total_incomplete} runs that need to be evaluated/rerun:")
    for model, runs in incomplete_runs.items():
        if runs:
            print(f"\n🤖 Model: {model}")
            for db, lang, _, reason in runs:
                print(f"  - {db} | {lang} | Reason: {reason}")
                
    if not api_key_set:
        print("\n❌ Error: OPENROUTER_API_KEY environment variable is not set.")
        print("Please set it in your terminal before running: export OPENROUTER_API_KEY='your-key'")
        print("You can also run the planned commands printed above manually with your key.")
        return
        
    print("\n🚀 Starting targeted runs...")
    
    for model, runs in incomplete_runs.items():
        if not runs:
            continue
            
        # Group runs by DB to execute them efficiently (optional, but run_bulk_evaluation.py accepts all files in one go!)
        # Let's join all sampled files for this model into a single comma-separated list
        files_list = ",".join(str(r[2]) for r in runs)
        
        print(f"\n🤖 Running targeted evaluations for {model}...")
        print(f"Files to evaluate: {len(runs)} files")
        
        cmd = [
            "python3.10", str(RUN_BULK_SCRIPT),
            "--files", files_list,
            "--model", model,
            "--workers", "6",
            "--disable-knowledge"
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            print(f"✅ Finished runs for {model}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error occurred while running evaluations for {model}: {e}")
            print("Moving to next model if any...")
            
    print("\n🎉 targeted evaluation run completed! Please verify results using the checker.")

if __name__ == "__main__":
    main()
