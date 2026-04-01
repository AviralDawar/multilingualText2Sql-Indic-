#!/usr/bin/env python3
import os
import csv
import argparse
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Plot COMET distribution for a single language.")
    parser.add_argument("--lang", type=str, help="Language to plot (e.g., 'Hindi', or 'All')")
    parser.add_argument("--batch", action="store_true", help="Generate plots for all unique languages found in the CSV")
    args = parser.parse_args()

    if not args.lang and not args.batch:
        parser.error("At least one of --lang or --batch is required")

    project_root = Path(__file__).resolve().parent.parent.parent
    scores_file = project_root / "output" / "comet_scores" / "task_wise_granular_scores.csv"

    # 1. Load Data
    all_rows = []
    with open(scores_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["lang"].lower() != "unknown":
                all_rows.append(row)

    unique_langs = sorted(list(set(r["lang"] for r in all_rows)))
    
    # 2. Determine what to plot
    to_process = []
    if args.batch:
        to_process = unique_langs
    elif args.lang:
        to_process = [args.lang]

    # 3. Plotting Loop
    for target in to_process:
        scores = []
        is_all = (target.lower().strip() == "all")
        
        for row in all_rows:
            if is_all or row["lang"].lower() == target.lower().strip():
                scores.append(float(row["comet_score"]))

        if not scores:
            print(f"Skipping '{target}': No records found.")
            continue

        # Statistical values
        mean_val = np.mean(scores)
        std_val = np.std(scores)
        tier1 = mean_val - std_val

        # Plotting
        plt.figure(figsize=(10, 6))
        counts, bins, patches = plt.hist(scores, bins=30, color='#4C72B0', edgecolor='black', alpha=0.7, label='Translation Density', density=True)
        plt.axvline(x=tier1, color='#C44E52', linestyle='--', linewidth=2.5, label=f'Suspicious (μ-1σ) = {tier1:.4f}')
        plt.fill_between([min(bins), tier1], 0, max(counts) * 1.05, color='#C44E52', alpha=0.15)

        if is_all:
            plt.title('COMET Score Distribution for All Combined', fontsize=16, fontweight='bold', pad=15)
        else:
            plt.title(f'COMET Score Distribution for {target}', fontsize=16, fontweight='bold', pad=15)

        plt.xlabel('COMET Quality Score', fontsize=12)
        plt.ylabel('Density', fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Stats box
        n_label = "11933" if is_all else str(len(scores))
        stats_text = f"Total (n) = {n_label}\nMean = {mean_val:.3f}\nStd Dev = {std_val:.3f}"
        plt.gca().text(0.05, 0.95, stats_text, transform=plt.gca().transAxes, fontsize=11, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
        plt.legend(loc='upper right', fontsize=11)

        out_dir = project_root / "output" / "comet_scores"
        out_file = out_dir / f"{target.lower().replace(' ', '_')}_comet_dist.png"
        plt.tight_layout()
        plt.savefig(out_file, dpi=300)
        plt.close()
        print(f"✅ Chart saved: {out_file.name}")

if __name__ == "__main__":
    main()
