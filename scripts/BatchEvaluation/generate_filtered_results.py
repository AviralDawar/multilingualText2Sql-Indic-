import json
import re
from pathlib import Path

# Project paths
PROJECT_ROOT = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
OUTPUT_DIR = PROJECT_ROOT / "output"

def load_knowledge(knowledge_path: Path) -> dict:
    knowledge_map = {}
    if not knowledge_path or not knowledge_path.exists():
        return knowledge_map
    with open(knowledge_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            for item in data:
                if 'pair_id' in item and 'evidence' in item:
                    knowledge_map[item['pair_id']] = item['evidence']
        except Exception as e:
            print(f"Error loading knowledge from {knowledge_path}: {e}")
    return knowledge_map

def resolve_knowledge_file(db_name: str) -> Path:
    patterns = [
        PROJECT_ROOT / "evidence_files" / f"{db_name}_evidence.json",
        PROJECT_ROOT / "evidence_files" / f"{db_name}_text2sql_evidence.json",
        OUTPUT_DIR / "knowledge_files_db" / f"{db_name}_evidence.json",
        OUTPUT_DIR / "knowledge_files_db" / f"{db_name}_text2sql_evidence.json"
    ]
    for p in patterns:
        if p.exists():
            return p
    import glob
    found = glob.glob(str(PROJECT_ROOT / f"evidence_files/*{db_name}*.json")) + glob.glob(str(OUTPUT_DIR / f"knowledge_files_db/*{db_name}*.json"))
    if found:
        return Path(found[0])
    return None

def process_results_file(results_file: Path):
    if not results_file.exists():
        print(f"File not found: {results_file}")
        return

    # Extract model slug from results_<model_slug>.md
    model_slug = results_file.stem.replace("results_", "")
    print(f"\nProcessing results file: {results_file.name} (Slug: {model_slug})")

    # Read the file line-by-line
    header_lines = []
    table_rows = []
    
    with open(results_file, "r") as f:
        for line in f:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 5 and parts[0] == "" and parts[-1] == "":
                db = parts[1]
                lang = parts[2]
                k_file = parts[3]
                res = parts[4]
                # Skip header and separator
                if db and db not in ["Database", "---"] and lang:
                    table_rows.append((db, lang, k_file, res))
            elif not table_rows:
                header_lines.append(line)

    if not table_rows:
        print("No valid table rows found.")
        return

    # Now, let's recalculate the scores ONLY for tasks that have evidence
    filtered_rows = []
    lang_map = {
        "English": "",
        "Hindi Romanized": "hinglish",
        "Hindi": "hindi",
        "Bengali": "bengali",
        "Tamil": "tamil",
        "Telugu": "telugu",
        "Marathi": "marathi",
        "Arabic": "arabic",
        "Spanish": "spanish",
        "Korean": "korean",
        "Italian": "italian",
        "French": "french",
        "Chinese": "chinese"
    }

    # Cache loaded knowledge files to avoid redundant disk reads
    knowledge_cache = {}

    for db, lang, k_file, orig_res in table_rows:
        # Load knowledge for this database
        if db not in knowledge_cache:
            k_path = resolve_knowledge_file(db)
            if k_path:
                knowledge_cache[db] = load_knowledge(k_path)
                print(f"Loaded evidence for {db}: {len(knowledge_cache[db])} entries resolved.")
            else:
                knowledge_cache[db] = {}
                print(f"Warning: No evidence file resolved for {db}!")

        k_map = knowledge_cache[db]

        # Resolve the evaluated file
        eval_dir = OUTPUT_DIR / db / f"eval_files_oneshot_{model_slug}"
        suffix = lang_map.get(lang, lang.lower())
        matched_file = None

        if eval_dir.exists():
            for f in eval_dir.glob("*.jsonl"):
                if suffix == "":
                    # Ensure no other language suffix is in the name
                    is_other = False
                    for s in lang_map.values():
                         if s and f"_{s}" in f.name.lower():
                             is_other = True
                             break
                    if not is_other and f.name.endswith("_evaluated.jsonl"):
                        matched_file = f
                        break
                else:
                    if f"_{suffix}_evaluated.jsonl" in f.name.lower():
                        matched_file = f
                        break

        if not matched_file or not matched_file.exists():
            print(f"⚠️  Evaluated file not found for {db} ({lang}). Keeping original.")
            filtered_rows.append((db, lang, k_file, orig_res))
            continue

        # Score only tasks that have evidence
        em_count = 0
        ex_count = 0
        total = 0

        with open(matched_file, "r") as infile:
            for line in infile:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    pair_id = data.get("pair_id")
                    
                    # We check if pair_id is in evidence file and the evidence is non-empty
                    if pair_id in k_map and k_map[pair_id].strip():
                        em_count += data.get("em", 0)
                        ex_count += data.get("ex", 0)
                        total += 1
                except Exception as e:
                    print(f"Error parsing task in {matched_file.name}: {e}")

        if total > 0:
            new_res = f"{em_count}/{total} (EM), {ex_count}/{total} (EX)"
            print(f"✅ {db} ({lang}): Filtered to {total} tasks (Orig score: {orig_res} -> Filtered: {new_res})")
            filtered_rows.append((db, lang, k_file, new_res))
        else:
            print(f"❌ {db} ({lang}): No tasks had evidence! Putting original score.")
            filtered_rows.append((db, lang, k_file, orig_res))

    # Write out the new filtered file
    filtered_file = results_file.parent / f"{results_file.stem}_filtered.md"
    with open(filtered_file, "w") as out:
        out.write(f"# Text2SQL Evaluation Results (FILTERED - WITH EVIDENCE ONLY): {model_slug.replace('_', '/')}\n")
        out.write("*(Evaluated tasks ONLY where corresponding schema evidence is present)*\n\n")
        out.write("| Database | Language | Knowledge File | Results (EM, EX) |\n")
        out.write("| --- | --- | --- | --- |\n")
        
        for db, lang, k_file, res in filtered_rows:
            out.write(f"| {db} | {lang} | {k_file} | {res} |\n")

    print(f"🎉 Successfully created filtered results file: {filtered_file.name}")

def main():
    target_files = [
        PROJECT_ROOT / "results_minimax_minimax_m2_7.md",
        PROJECT_ROOT / "results_deepseek_deepseek_v3_2.md",
        PROJECT_ROOT / "results_meta_llama_llama_3_3_70b_instruct.md"
    ]
    for tf in target_files:
        if tf.exists():
            process_results_file(tf)

if __name__ == "__main__":
    main()
