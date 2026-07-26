#!/bin/bash

# ==============================================================================
# Qwen Text-to-SQL with Evidence Resumption Script
# ==============================================================================

# Check for API Key
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo "❌ Error: OPENROUTER_API_KEY environment variable is not set."
    echo "Please run: export OPENROUTER_API_KEY='your-key'"
    exit 1
fi

# Run the python resume script
python3.10 scripts/BatchEvaluation/resume_evaluation_with_evidence.py
