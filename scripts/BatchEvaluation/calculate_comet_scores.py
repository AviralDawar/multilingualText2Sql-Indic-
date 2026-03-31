#!/usr/bin/env python3
import os
import json
import csv
import argparse
from pathlib import Path
from collections import defaultdict

# COMET imports
try:
    from comet import download_model, load_from_checkpoint
except ImportError:
    print("Error: The 'unbabel-comet' library is not installed.")
    print("Please run: pip install unbabel-comet")
    exit(1)

def main():
    parser = argparse.ArgumentParser(description="Calculate reference-free COMET scores for translations.")
    parser.add_argument("--batch-size", type=int, default=8, help="Batch size for COMET evaluation (decrease if OOM)")
    parser.add_argument("--gpus", type=int, default=0, help="Number of GPUs to use (set 0 for CPU/Mac)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent
    output_dir = project_root / "output"
    
    # New folder for storing results
    comet_output_dir = project_root / "output" / "comet_scores"
    comet_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Scanning {output_dir} for sampled tasks...")

    # 1. Gather all tasks
    lang_tasks = [] # list of dicts: {'db': str, 'lang': str, 'pair_id': str, 'src': str, 'mt': str}
    
    LANGUAGES = ["hindi", "bengali", "tamil", "telugu", "marathi", "hinglish"]

    for db_dir in output_dir.iterdir():
        if not db_dir.is_dir(): continue
        sampled_dir = db_dir / "sampled_tasks"
        if not sampled_dir.exists(): continue

        jsonl_files = list(sampled_dir.glob("*.jsonl"))
        if not jsonl_files: continue
        
        # Find English master (file without any language suffix)
        english_file = None
        for f in jsonl_files:
            if not any(lang in f.stem.lower() for lang in LANGUAGES):
                english_file = f
                break
        
        if not english_file:
            print(f"  [Skip] {db_dir.name} - Could not identify English master file.")
            continue

        # Load English mapping (Source Text)
        english_mapping = {}
        with open(english_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                data = json.loads(line)
                english_mapping[data["pair_id"]] = data["question"]

        # Load translations (Machine Translation Text)
        for f in jsonl_files:
            if f == english_file: continue
            
            # Detect language
            lang = "Unknown"
            for l in LANGUAGES:
                if l in f.stem.lower():
                    lang = "Hindi Romanized" if l == "hinglish" else l.capitalize()
                    break
            
            with open(f, "r", encoding="utf-8") as infile:
                for line in infile:
                    if not line.strip(): continue
                    data = json.loads(line)
                    pair_id = data["pair_id"]
                    
                    if pair_id in english_mapping:
                        lang_tasks.append({
                            "db": db_dir.name,
                            "lang": lang,
                            "pair_id": pair_id,
                            "src": english_mapping[pair_id],
                            "mt": data["question"]
                        })

    if not lang_tasks:
        print("No paired translation tasks found in output/*/sampled_tasks/")
        return

    print(f"\nTotal translation pairs mapped: {len(lang_tasks)}")
    print("\nDownloading and loading COMET model (wmt22-cometkiwi-da)...")
    print("*(This 2.2GB model requires a HuggingFace authentication token because it is gated. Please ensure HF_TOKEN is exported!)*")
    
    # 2. Load Model
    model_path = download_model("Unbabel/wmt22-cometkiwi-da")
    model = load_from_checkpoint(model_path)

    # 3. Format Data and Predict Scores
    comet_data = [{"src": task["src"], "mt": task["mt"]} for task in lang_tasks]

    print(f"\nComputing COMET scores (Batch Size: {args.batch_size}, GPUs: {args.gpus})...")
    # Depending on Unbabel-COMET version, predict() returns either an object or a namedtuple
    results = model.predict(comet_data, batch_size=args.batch_size, gpus=args.gpus, num_workers=2)
    
    scores_list = results.scores if hasattr(results, 'scores') else results['scores']

    for task, score in zip(lang_tasks, scores_list):
        task["comet_score"] = float(score)

    # 4. Aggregate and Export Results
    lang_scores = defaultdict(list)
    for task in lang_tasks:
        lang_scores[task["lang"]].append(task["comet_score"])

    # Write overall summary per language
    summary_file = comet_output_dir / "language_summary_scores.md"
    with open(summary_file, "w", encoding="utf-8") as f:
        print("\n" + "="*40)
        print("AVERAGE COMET SCORES PER LANGUAGE")
        print("="*40)
        f.write("# Average COMET Scores by Language\n\n")
        f.write("| Language | Average COMET Score | Total Pairs Evaluated |\n")
        f.write("|---|---|---|\n")
        
        for lang, scores in sorted(lang_scores.items()):
            avg_score = sum(scores) / len(scores)
            f.write(f"| {lang} | {avg_score:.4f} | {len(scores)} |\n")
            print(f"{lang.ljust(10)}: {avg_score:.4f}  (from {len(scores)} tasks)")
        
        print("="*40)

    # Write granular task-wise results so user can filter out bad translations
    csv_file = comet_output_dir / "task_wise_granular_scores.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        # Save as CSV for easy filtering/sorting in Excel or Pandas
        fieldnames = ["db", "lang", "pair_id", "comet_score", "src", "mt"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for task in lang_tasks:
            writer.writerow(task)
            
    print(f"\n✅ Success! Saved individual task-wise scores to: {csv_file}")
    print(f"✅ Success! Saved summary report to: {summary_file}")

if __name__ == "__main__":
    main()
