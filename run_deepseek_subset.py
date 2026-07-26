#!/usr/bin/env python3
import os
import shutil
import subprocess
from pathlib import Path

# ==============================================================================
# Text-to-SQL DeepSeek Evaluation Orchestrator (Spanish, Italian, French)
# ==============================================================================

PROJECT_ROOT = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
OUTPUT_DIR = PROJECT_ROOT / "output"
RUN_BULK_SCRIPT = PROJECT_ROOT / "scripts" / "BatchEvaluation" / "run_bulk_evaluation.py"

DATABASES = [
    "INDIA_Economic_Census_Firms",
    "INDIA_ICRISAT_District_Level_Agricultural_Data",
    "INDIA_PRIMARY_POPULATION_CENSUS_1991"
]

LANGUAGES = ["spanish", "italian", "french"]
MODEL = "deepseek/deepseek-v3.2"
MODEL_SLUG = "deepseek_deepseek_v3_2"

def get_sampled_task_file(db: str, lang: str) -> Path:
    sampled_dir = OUTPUT_DIR / db / "sampled_tasks"
    if not sampled_dir.exists():
        return None
    files = list(sampled_dir.glob(f"*_{lang}.jsonl"))
    return files[0] if files else None

def clear_existing_evaluations():
    print("🧹 Cleaning and backing up previous evaluations for Spanish, Italian, and French...")
    for db in DATABASES:
        for suffix in ["", "_no_evidence"]:
            eval_dir = OUTPUT_DIR / db / f"eval_files_oneshot_{MODEL_SLUG}{suffix}"
            if not eval_dir.exists():
                continue
                
            for lang in LANGUAGES:
                eval_files = list(eval_dir.glob(f"*_{lang}_evaluated.jsonl"))
                for ef in eval_files:
                    backup_path = ef.with_suffix(".jsonl.bak")
                    print(f"  - Backing up: {ef.relative_to(PROJECT_ROOT)} -> {backup_path.name}")
                    shutil.move(str(ef), str(backup_path))

def run_evaluations(disable_knowledge: bool):
    scenario = "WITHOUT EVIDENCE" if disable_knowledge else "WITH EVIDENCE"
    print(f"\n======================================================================")
    print(f"🚀 Running Evaluations: {scenario}")
    print(f"======================================================================")
    
    # We loop databases and languages sequentially with a low worker count (3)
    # to guarantee database connection stability and avoid PostgreSQL drops
    for db in DATABASES:
        for lang in LANGUAGES:
            task_file = get_sampled_task_file(db, lang)
            if not task_file or not task_file.exists():
                print(f"⚠️ Warning: Task file not found for {db} | {lang}")
                continue
                
            print(f"\n🔄 Running {db} | {lang.upper()} ({scenario})...")
            cmd = [
                "python3.10", str(RUN_BULK_SCRIPT),
                "--files", str(task_file),
                "--model", MODEL,
                "--workers", "3"
            ]
            if disable_knowledge:
                cmd.append("--disable-knowledge")
                
            print(f"Executing: {' '.join(cmd)}")
            subprocess.run(cmd)

def main():
    if not os.environ.get("OPENROUTER_API_KEY"):
        print("❌ Error: OPENROUTER_API_KEY environment variable is not set.")
        print("Please export it: export OPENROUTER_API_KEY='your-key'")
        return
        
    # 1. Clear previous evaluations to force a fresh run on the exact same tasks
    clear_existing_evaluations()
    
    # 2. Run Scenario 1: With Evidence
    run_evaluations(disable_knowledge=False)
    
    # 3. Run Scenario 2: Without Evidence
    run_evaluations(disable_knowledge=True)
    
    print("\n======================================================================")
    print("🎉 All evaluations completed successfully!")
    print("======================================================================")
    
    # 4. Regenerate reports
    print("\n📊 Compiling baseline and delta reports...")
    subprocess.run(["python3.10", str(PROJECT_ROOT / "compile_results_with_evidence.py")])
    subprocess.run(["python3.10", str(PROJECT_ROOT / "compile_comparative_report.py")])
    print("🎉 Done! All markdown tables updated successfully.")

if __name__ == "__main__":
    main()
