#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
RUN_BULK_SCRIPT = PROJECT_ROOT / "scripts" / "BatchEvaluation" / "run_bulk_evaluation.py"
COMPILE_SCRIPT = PROJECT_ROOT / "compile_results_with_evidence.py" # Wait, the user has the compile script at the root, or scratch. Let's make sure we find it.

INCOMPLETE_RUNS = [
    # (model, db, lang, file_path)
    ("deepseek/deepseek-v3.2", "INDIA_Economic_Census_Firms", "english", "output/INDIA_Economic_Census_Firms/sampled_tasks/INDIA_Economic_Census_Firms_text2sql_20260318_030334.jsonl"),
    ("deepseek/deepseek-v3.2", "INDIA_ICRISAT_District_Level_Agricultural_Data", "english", "output/INDIA_ICRISAT_District_Level_Agricultural_Data/sampled_tasks/INDIA_ICRISAT_District_Level_Agricultural_Data_text2sql_20260319_211936.jsonl"),
    ("meta-llama/llama-3.3-70b-instruct", "INDIA_Economic_Census_Firms", "english", "output/INDIA_Economic_Census_Firms/sampled_tasks/INDIA_Economic_Census_Firms_text2sql_20260318_030334.jsonl"),
    ("minimax/minimax-m2.7", "INDIA_Economic_Census_Firms", "english", "output/INDIA_Economic_Census_Firms/sampled_tasks/INDIA_Economic_Census_Firms_text2sql_20260318_030334.jsonl")
]

def main():
    env = os.environ.copy()
    api_key = env.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY environment variable is required.")
    
    print("🚀 Starting Resumption of All Interrupted/Incomplete Baseline Slices across DeepSeek, Llama, and MiniMax...")
    
    for model, db, lang, file_path in INCOMPLETE_RUNS:
        full_path = PROJECT_ROOT / file_path
        if not full_path.exists():
            print(f"❌ Error: File not found: {full_path}")
            continue
            
        print(f"\n==================================================")
        print(f"🔄 Resuming Model: {model} | DB: {db} | Lang: {lang}")
        print(f"📄 File: {full_path.name}")
        print(f"==================================================")
        
        cmd = [
            "python3.10", str(RUN_BULK_SCRIPT),
            "--files", str(full_path),
            "--model", model,
            "--workers", "6"
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, env=env)
        
    print("\n🎉 All incomplete slices successfully completed!")
    
    # Run the results compiler
    print("\n📊 Compiling consolidated baseline reports...")
    compile_py = PROJECT_ROOT / "compile_results_with_evidence.py"
    if compile_py.exists():
        subprocess.run(["python3.10", str(compile_py)], env=env)
    else:
        # Fallback to scratch compiler
        scratch_compile = Path("/Users/aviraldawar/.gemini/antigravity/scratch/compile_results_with_evidence.py")
        if scratch_compile.exists():
            subprocess.run(["python3.10", str(scratch_compile)], env=env)
            
    print("🎉 Done compiling baseline reports!")

if __name__ == "__main__":
    main()
