#!/usr/bin/env python3
import os
import csv
import numpy as np
from pathlib import Path
from collections import defaultdict

def main():
    project_root = Path(__file__).resolve().parent.parent.parent
    comet_output_dir = project_root / "output" / "comet_scores"
    scores_file = comet_output_dir / "task_wise_granular_scores.csv"

    if not scores_file.exists():
        print(f"Error: Could not find scores file at {scores_file}")
        print("Please ensure calculate_comet_scores.py has been run successfully.")
        return

    # 1. Read all scores
    all_tasks = []
    lang_scores = defaultdict(list)
    lang_tasks = defaultdict(list)

    with open(scores_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["lang"].lower() == "unknown":
                continue
            score = float(row["comet_score"])
            row["comet_score"] = score  # Cast to float for easier sorting
            lang = row["lang"]
            
            all_tasks.append(row)
            lang_scores[lang].append(score)
            lang_tasks[lang].append(row)

    if not all_tasks:
        print("No tasks found in the CSV!")
        return

    # 2. Analyze Percentiles per Language
    print("\n" + "="*80)
    print(" " * 20 + "COMET DISTRIBUTION ANALYSIS & τ-FILTERING")
    print("="*80)

    # Calculate global variance
    all_scores = [t['comet_score'] for t in all_tasks]
    global_tau = np.percentile(all_scores, 20)

    print(f"\n[GLOBAL DATASET]")
    print(f"Total Translation Pairs : {len(all_tasks)}")
    print(f"Global Mean Score       : {np.mean(all_scores):.4f}")
    print(f"Global Variance (Std)   : {np.std(all_scores):.4f}")
    print(f"Global 20th Pctl (τ)    : {global_tau:.4f}")

    high_risk_tasks = []

    print("\n[PER-LANGUAGE ANALYSIS]")
    for lang in sorted(lang_scores.keys()):
        scores = lang_scores[lang]
        tasks = lang_tasks[lang]
        
        mean_val = np.mean(scores)
        std_val = np.std(scores)
        # Calculate Tau at the 20th percentile (bottom quintile)
        tau_20 = np.percentile(scores, 20)
        
        # Filter high risk subset
        undershoot_tasks = [t for t in tasks if t['comet_score'] <= tau_20]
        # Sort lowest scores first
        undershoot_tasks.sort(key=lambda x: x['comet_score'])
        
        high_risk_tasks.extend(undershoot_tasks)

        print(f"\n➤ {lang} (n={len(scores)})")
        print(f"   Mean: {mean_val:.4f}  |  StdDev: {std_val:.4f}")
        print(f"   Threshold (τ)   : {tau_20:.4f}")
        print(f"   High-risk count : {len(undershoot_tasks)} tasks (Bottom 20%)")

    # 3. Export High-Risk Samples for Human Auditing
    output_audit_file = comet_output_dir / "audit_high_risk_bottom_20_percentile.csv"
    
    # Sort globally by lowest score first
    high_risk_tasks.sort(key=lambda x: x['comet_score'])

    with open(output_audit_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["db", "lang", "pair_id", "comet_score", "src", "mt"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for task in high_risk_tasks:
            writer.writerow(task)

    print("\n" + "="*80)
    print("RESEARCH METHODOLOGY IMPLEMENTED:")
    print("Following the τ-filtering methodology, the bottom quintile of translations")
    print(f"have been identified as 'high-risk' samples. By auditing the 20th percentile,")
    print(f"we ensured that exactly {len(high_risk_tasks)} potentially sub-optimal tasks can be human-verified.")
    print("="*80)
    
    print(f"\n✅ Successfully exported {len(high_risk_tasks)} tasks for human auditing to:")
    print(f"   {output_audit_file}")

if __name__ == "__main__":
    main()
