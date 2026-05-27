#!/bin/bash

# ==============================================================================
# Text2SQL Multilingual Evaluation Automation Script (WITHOUT SCHEMA EVIDENCE)
# ==============================================================================

# Exit immediately if a command exits with a non-zero status
set -e

# Target Databases
DATABASES=(
    "INDIA_Economic_Census_Firms"
    "INDIA_ICRISAT_District_Level_Agricultural_Data"
    "INDIA_PRIMARY_POPULATION_CENSUS_1991"
)

# Target Languages (6 Foreign Languages + English)
LANGUAGES=("english" "arabic" "spanish" "korean" "italian" "french" "chinese")

# Target OpenRouter Models (All 4 major models evaluated)
MODELS=(
    "deepseek/deepseek-v3.2"
    "meta-llama/llama-3.3-70b-instruct"
    "minimax/minimax-m2.7"
    "qwen/qwen3-8b"
)

# Workers for parallel API generation
WORKERS=10

# Check for API Key
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo "❌ Error: OPENROUTER_API_KEY environment variable is not set."
    echo "Please set it before running: export OPENROUTER_API_KEY='your-key'"
    exit 1
fi

echo "======================================================================"
echo " Starting Bulk Evaluation (WITHOUT EVIDENCE) for 3 Databases"
echo "======================================================================"
echo "Databases:  ${DATABASES[*]}"
echo "Languages:  ${LANGUAGES[*]}"
echo "Models:     ${MODELS[*]}"
echo "======================================================================"

# Loop through each model
for model in "${MODELS[@]}"; do
    echo -e "\n🤖 Evaluating model: $model"
    echo "----------------------------------------------------------------------"
    
    # Loop through each database
    for db in "${DATABASES[@]}"; do
        echo -e "\n📂 Resolving files for database: $db"
        
        # Build the files list dynamically by scanning the sampled_tasks directory
        FILES_LIST=""
        for lang in "${LANGUAGES[@]}"; do
            FILE=""
            if [ "$lang" = "english" ]; then
                # Find base English file (does not end in foreign or Indic language suffixes)
                FILE=$(ls output/${db}/sampled_tasks/*.jsonl 2>/dev/null | \
                       grep -v -E "_(arabic|spanish|korean|italian|french|chinese|bengali|hindi|hinglish|marathi|tamil|telugu)\.jsonl" | \
                       head -n 1 || true)
            else
                # Use globbing to find the exact language file
                FILE=$(ls output/${db}/sampled_tasks/*_${lang}.jsonl 2>/dev/null | head -n 1 || true)
            fi
            
            if [ -n "$FILE" ]; then
                if [ -z "$FILES_LIST" ]; then
                    FILES_LIST="$FILE"
                else
                    FILES_LIST="$FILES_LIST,$FILE"
                fi
                echo "  Found file for $lang: $FILE"
            else
                echo "  ⚠️ Warning: File for language '$lang' not found in output/${db}/sampled_tasks/"
            fi
        done
        
        # If no files found for this DB, skip it
        if [ -z "$FILES_LIST" ]; then
            echo "❌ No matching task files found for $db. Skipping."
            continue
        fi
        
        # Execute the python evaluation runner with --disable-knowledge
        echo "🚀 Running bulk evaluation WITHOUT evidence..."
        python3.10 scripts/BatchEvaluation/run_bulk_evaluation.py \
            --files "$FILES_LIST" \
            --model "$model" \
            --workers "$WORKERS" \
            --disable-knowledge
            
        echo "✅ Finished evaluation for $db without evidence"
    done
    
    echo -e "\n🌟 Complete evaluation table updated for model: $model (saved to results_<model>_no_evidence.md)"
done

echo -e "\n🎉 All batch evaluations WITHOUT evidence have successfully completed!"
