#!/usr/bin/env python3
import os
import json
import random
import subprocess
import argparse
from pathlib import Path
from typing import List, Dict, Set

# Configuration
DATABASES = [
    "INDIA_NWMP_Water_Quality_Data",
    "INDIA_UDISE_Right_To_Education_RTE_and_School_Management_data",
    "INDIA_ROAD_ACCIDENTS_DATASET_2001",
    "INDIA_Village_Amenities_Directory_2001",
    "INDIA_PRIMARY_POPULATION_CENSUS_1991",
    "INDIA_HMIS_Sub_District_Report_Facility_wise",
    "INDIA_Economic_Census_Firms",
    "INDIA_IHDS_2011_TRACKING_SURVEY",
    "INDIA_HMIS_Sub_District_Report_Rural_Urban",
    "INDIA_ICRISAT_District_Level_Agricultural_Data",
    "INDIA_IHDS_2005_HOUSEHOLD_SURVEY",
    "INDIA_IHDS_2005_INDIVIDUAL_SURVEY"
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
ONEShot_SCRIPT = PROJECT_ROOT / "scripts" / "OneShot_FewShot" / "run_oneshot.py"
KNOWLEDGE_DIR = OUTPUT_DIR / "knowledge_files_db"

def get_task_files(db_name: str) -> List[Path]:
    task_dir = OUTPUT_DIR / db_name / "task_files"
    if not task_dir.exists():
        print(f"Warning: Task directory not found for {db_name}: {task_dir}")
        return []
    return list(task_dir.glob("*.jsonl"))

def sample_tasks(db_name: str, task_files: List[Path], sample_size: int = 100) -> Path:
    """Sample 100 tasks consistently across all languages for a DB or reuse existing."""
    sampled_dir = OUTPUT_DIR / db_name / "sampled_tasks"
    if sampled_dir.exists() and any(sampled_dir.glob("*.jsonl")):
        print(f"Skipping sampling for {db_name}, using existing files in {sampled_dir}")
        return sampled_dir

    # Find English file (no language suffix)
    english_file = None
    for f in task_files:
        if not any(lang in f.stem.lower() for lang in ["hindi", "bengali", "tamil", "telugu", "marathi", "hinglish"]):
            english_file = f
            break
    
    if not english_file and task_files:
        english_file = task_files[0]
        print(f"Warning: Could not identify English master for {db_name}, using {english_file}")
    
    if not english_file:
        return None

    # Load all pair_ids from english file
    with open(english_file, "r") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    
    if not lines:
        return None

    sampled_lines = random.sample(lines, min(len(lines), sample_size))
    sampled_ids = {line["pair_id"] for line in sampled_lines}

    # Create sampled directory
    sampled_dir = OUTPUT_DIR / db_name / "sampled_tasks"
    sampled_dir.mkdir(parents=True, exist_ok=True)

    for f in task_files:
        sampled_task_file = sampled_dir / f.name
        with open(f, "r") as infile, open(sampled_task_file, "w") as outfile:
            for line in infile:
                data = json.loads(line)
                if data.get("pair_id") in sampled_ids:
                    outfile.write(json.dumps(data, ensure_ascii=False) + "\n")
    
    return sampled_dir

def run_evaluation(db_name: str, sampled_dir: Path, args):
    print(f"\n>>> Evaluating {db_name} with model {args.model}...")
    
    # Model-specific output folder
    model_slug = args.model.replace("/", "_").replace("-", "_").replace(".", "_")
    eval_output_dir = OUTPUT_DIR / db_name / f"eval_files_oneshot_{model_slug}"
    eval_output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "python3.10", str(ONEShot_SCRIPT),
        "--database", db_name,
        "--database-dir", str(PROJECT_ROOT / "databases"),
        "--input", str(sampled_dir),
        "--output", str(eval_output_dir),
        "--provider", args.provider,
        "--model", args.model,
        "--workers", str(args.workers),
        "--pg-db", "indicdb"
    ]
    
    if args.api_key:
        cmd += ["--api-key", args.api_key]

    # Special case for NWMP knowledge files
    if "NWMP" in db_name:
        knowledge_file = KNOWLEDGE_DIR / f"{db_name}_evidence.json"
        if knowledge_file.exists():
            cmd += ["--knowledge", str(knowledge_file)]

    # Removed capture_output=True to allow progress visibility (tqdm) in terminal
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Error evaluating {db_name} (Exit code {result.returncode})")
    return result

def parse_results(db_name: str, model_slug: str) -> Dict[str, str]:
    """Parse results for a DB from its model-specific eval folder."""
    eval_dir = OUTPUT_DIR / db_name / f"eval_files_oneshot_{model_slug}"
    if not eval_dir.exists():
        print(f"Warning: Eval directory not found for {db_name}: {eval_dir}")
        return {}
    
    stats = {}
    for f in eval_dir.glob("*_evaluated.jsonl"):
        # Identify language from filename
        lang = "English"
        for l in ["hindi", "bengali", "tamil", "telugu", "marathi", "hinglish"]:
            if l in f.name.lower():
                lang = l.capitalize()
                break
        
        em_count = 0
        ex_count = 0
        total = 0
        try:
            with open(f, "r") as infile:
                for line in infile:
                    if not line.strip(): continue
                    data = json.loads(line)
                    em_count += data.get("em", 0)
                    ex_count += data.get("ex", 0)
                    total += 1
            
            if total > 0:
                stats[lang] = f"{em_count}/{total} (EM), {ex_count}/{total} (EX)"
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    return stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--provider", default="openrouter")
    parser.add_argument("--model", default="deepseek/deepseek-v3.2")
    parser.add_argument("--api-key")
    parser.add_argument("--limit", type=int, help="Limit number of DBs for testing")
    parser.add_argument("--dbs", help="Comma-separated list of DB names to run")
    args = parser.parse_args()

    if not args.api_key and not os.environ.get("OPENROUTER_API_KEY"):
        # Fallback to known key if none provided
        args.api_key = "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"

    all_stats = {}
    if args.dbs:
        dbs_to_run = [db.strip() for db in args.dbs.split(",")]
    else:
        dbs_to_run = DATABASES[:args.limit] if args.limit else DATABASES
    model_slug = args.model.replace("/", "_").replace("-", "_").replace(".", "_")

    for db_name in dbs_to_run:
        task_files = get_task_files(db_name)
        if not task_files:
            continue
        
        sampled_dir = sample_tasks(db_name, task_files)
        if not sampled_dir:
            continue
        
        run_evaluation(db_name, sampled_dir, args)
        all_stats[db_name] = parse_results(db_name, model_slug)

        # Generate/Update model-specific results file incrementally
        results_file = PROJECT_ROOT / f"results_{model_slug}.md"
        with open(results_file, "w") as f:
            f.write(f"# Text2SQL Evaluation Results: {args.model}\n")
            f.write(f"*(Sampled 100 questions per DB - Updated after {db_name})*\n\n")
            f.write("| Database | Language | Results (EM, EX) |\n")
            f.write("| --- | --- | --- |\n")
            
            for db in sorted(all_stats.keys()):
                langs = ["English", "Hinglish", "Hindi", "Bengali", "Tamil", "Telugu", "Marathi"]
                for lang in langs:
                    res = all_stats[db].get(lang, "N/A")
                    f.write(f"| {db} | {lang} | {res} |\n")
        
        print(f"Incremental results saved to {results_file}")
    
    print(f"\nFull evaluation complete. Final results saved to {results_file}")

if __name__ == "__main__":
    main()
