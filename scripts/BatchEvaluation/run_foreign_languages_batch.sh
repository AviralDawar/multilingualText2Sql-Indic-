#!/bin/bash

# ==============================================================================
# Text2SQL Multilingual Evaluation Automation Script (Non-Indic Foreign Languages)
# ==============================================================================

# Exit immediately if a command exits with a non-zero status
set -e

# Target Databases
DATABASES=(
    "INDIA_PRIMARY_POPULATION_CENSUS_1991"
    "INDIA_Economic_Census_Firms"
    "INDIA_HMIS_Sub_District_Report_Facility_wise"
    "INDIA_IHDS_2005_HOUSEHOLD_SURVEY"
    "INDIA_ICRISAT_District_Level_Agricultural_Data"
)

# Target Languages
LANGUAGES=("arabic" "spanish" "korean" "italian" "french" "chinese")

# Target OpenRouter Models
MODELS=(
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
echo " Starting Bulk Evaluation for 5 Databases, 6 Languages, and 1 Model"
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
            # Use globbing to find the exact timestamped task file
            FILE=$(ls output/${db}/sampled_tasks/*_${lang}.jsonl 2>/dev/null | head -n 1 || true)
            
            if [ -n "$FILE" ]; then
                if [ -z "$FILES_LIST" ]; then
                    FILES_LIST="$FILE"
                else
                    FILES_LIST="$FILES_LIST,$FILE"
                fi
            else
                echo "⚠️  Warning: File for language '$lang' not found in output/${db}/sampled_tasks/"
            fi
        done
        
        # If no files found for this DB, skip it
        if [ -z "$FILES_LIST" ]; then
            echo "❌ No matching task files found for $db. Skipping."
            continue
        fi
        
        # Execute the python evaluation runner
        echo "🚀 Running bulk evaluation..."
        python3.10 scripts/BatchEvaluation/run_bulk_evaluation.py \
            --files "$FILES_LIST" \
            --model "$model" \
            --workers "$WORKERS"
            
        echo "✅ Finished evaluation for $db"
    done
    
    echo -e "\n🌟 Complete evaluation table updated for model: $model"
done

echo -e "\n🎉 All batch evaluations have successfully completed!"
