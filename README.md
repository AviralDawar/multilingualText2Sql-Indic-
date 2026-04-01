# IndicDB

IndicDB is a research repository for **benchmarking multilingual Text-to-SQL on Indian public datasets**. It packages a collection of normalized Indian databases, multilingual question sets, schema-aware evidence generation, and evaluation pipelines used for the project described in [IndicDB - Benchmarking Multilingual Text-to-SQL Capabilities in Indian Languages](./IndicDB%20-%20Benchmarking%20Multilingual%20Text-to-SQL%20Capabilities%20in%20Indian%20Languages.pdf).

The core idea is simple: ask Text-to-SQL systems to answer questions in Indian languages while the underlying schemas, column names, and database values remain mostly English and highly relational. That creates pressure on translation, schema linking, value grounding, join reasoning, and execution correctness at the same time.

## What This Repository Contains

- A benchmark suite built from **20 Indian-domain relational databases**.
- **237 tables** and **1,465 columns** across the current released schemas.
- **7 evaluation languages/forms** in current outputs: English, Hindi, Bengali, Tamil, Telugu, Marathi, and Hinglish.
- Scripts for database construction, synthetic task generation, multilingual translation, evidence generation, one-shot/few-shot evaluation, metric recomputation, and translation-quality auditing.
- Stored outputs for model runs, task files, sampled subsets, evaluation JSONLs, COMET analysis, and metric summaries.

## Why IndicDB Is Hard

- Schemas are deliberately normalized and multi-table, averaging **11.85 tables per database** in the current release.
- Questions are multilingual, but schemas and many database values are English-coded.
- Datasets span real public domains such as census, education, health, agriculture, employment, household surveys, and transport.
- The benchmark stresses both logical reasoning and practical execution: joins, filters, aggregations, nested SQL, and entity/value grounding.

## Benchmark Scope

Current repository statistics from the schema notes in [`paper_content/DB_stats.md`](paper_content/DB_stats.md):

| Scope | Value |
| --- | ---: |
| Databases | 20 |
| Tables | 237 |
| Columns | 1,465 |
| Avg. tables per DB | 11.85 |
| Avg. columns per table | 6.18 |

Domain spread in the current release:

| Domain | DBs |
| --- | ---: |
| Household & Social Surveys | 6 |
| Census & Demography | 4 |
| Education | 3 |
| Health & Public Health | 3 |
| Economy & Employment | 2 |
| Agriculture | 1 |
| Transport & Safety | 1 |

## Repository Layout

```text
IndicDB/
|-- databases/                  # Normalized database packages (DDL, CSVs, sample JSONs, schema docs)
|-- output/                     # Task files, sampled subsets, eval JSONLs, metric summaries, COMET outputs
|-- results/                    # Consolidated markdown result tables
|-- evidence_files/             # Language-specific evidence files used during evaluation
|-- knowledge_files/            # Database knowledge artifacts and SQL-oriented notes
|-- paper_content/              # Paper support notes, stats, schema diagrams, DDL snippets
|-- scripts/
|   |-- BatchEvaluation/        # Batch runners, metric recomputation, COMET analysis
|   |-- DataSplitScripts/       # Older dataset-specific splitters
|   |-- LanguageConversionScripts/
|   |-- OneShot_FewShot/        # Inference and evaluation entrypoints
|   |-- QueryGeneration/        # Gold/task query generation and validation helpers
|   |-- CHESS/                  # Included agentic SQL synthesis package and adapters
|   |-- dsqg_syn/               # DSQG-Syn synthetic Text-to-SQL generation pipeline
|   `-- *.py                    # Main schema, loading, knowledge, and orchestration scripts
|-- config/                     # Credential templates and local connection configs
|-- PROMPTS_README.md           # Prompt snippets used in synthesis workflows
`-- IndicDB - Benchmarking Multilingual Text-to-SQL Capabilities in Indian Languages.pdf
```

## Main Workflows

### 1. Build A New Database

IndicDB includes a CSV-to-relational pipeline that turns a raw wide file into a normalized PostgreSQL-ready benchmark package:

```text
raw CSV
  -> LLM schema design
  -> schema parser
  -> table splitting
  -> DDL generation
  -> sample JSON generation
  -> PostgreSQL schema creation
  -> data loading
```

The schema-design logic follows the project notes in [`paper_content/DB_creationLogic.md`](paper_content/DB_creationLogic.md): an **Architect -> Auditor -> Refiner** pattern proposes a DIM/FACT-style relational design, checks normalization and joinability, and emits parser-friendly schema descriptions.

### 2. Generate Text-to-SQL Tasks

The repository supports both curated/gold workflows and synthetic generation:

- `scripts/QueryGeneration/` contains SQL generation and validation helpers.
- `scripts/dsqg_syn/` contains the DSQG-Syn synthetic data generation pipeline.
- `PROMPTS_README.md` stores the prompt blocks used for synthesis and NLQ generation.

### 3. Translate Tasks Into Indian Languages

The multilingual pipeline translates English task files into Indian languages while trying to preserve SQL-critical schema tokens and values. Current translation tooling lives under `scripts/LanguageConversionScripts/`.

### 4. Add Evidence / Knowledge

IndicDB supports two kinds of supporting context:

- **Database-level knowledge files** from `scripts/create_knowledge_file.py`.
- **Question-level evidence** from `scripts/SEED.py`.

These artifacts are used to help models ground domain terms and schema semantics during inference.

### 5. Evaluate Models

The main evaluation loop uses:

- `scripts/OneShot_FewShot/run_oneshot.py`
- `scripts/OneShot_FewShot/run_fewshot.py`
- `scripts/BatchEvaluation/run_bulk_evaluation.py`
- `scripts/sql_eval_utils.py` for normalized SQL exact-match recomputation

The repository already includes one-shot evaluation outputs for several models, including:

- `deepseek/deepseek-v3.2`
- `meta-llama/llama-3.3-70b-instruct`
- `minimax/minimax-m2.7`
- `qwen/qwen3-8b`

## Setup

### Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### PostgreSQL Config

Most active data-loading and evaluation scripts in this repo target **PostgreSQL**.

Create `config/postgres_credential.json` from the template:

```json
{
  "host": "localhost",
  "port": 5432,
  "user": "your_username",
  "password": "your_password",
  "database": "indicdb"
}
```

The tracked template is available at [`config/postgres_config.template.json`](config/postgres_config.template.json).

There is also a Snowflake template at [`config/snowflake_config.template.json`](config/snowflake_config.template.json), but the current database creation and benchmark evaluation scripts are centered on PostgreSQL.

## Quick Start

### Build Or Refresh A Database Package

Run the all-in-one orchestrator:

```bash
python3 scripts/pipeline.py \
  --database INDIA_UDISE_SCHOOL_PROFILES \
  --input databases/INDIA_UDISE_SCHOOL_PROFILES/INDIA_UDISE_SCHOOL_PROFILES/data/total_data.csv \
  --full \
  --use-database indicdb
```

Or run the steps individually:

```bash
python3 scripts/create_schema_llm_judge.py \
  databases/INDIA_UDISE_SCHOOL_PROFILES/INDIA_UDISE_SCHOOL_PROFILES/data/total_data.csv \
  --backend openrouter \
  --model deepseek/deepseek-v3.2 \
  --reasoning

python3 scripts/parse_schema_to_yaml.py \
  --schema databases/INDIA_UDISE_SCHOOL_PROFILES/INDIA_UDISE_SCHOOL_PROFILES/schema_info.md \
  --csv databases/INDIA_UDISE_SCHOOL_PROFILES/INDIA_UDISE_SCHOOL_PROFILES/data/total_data.csv \
  -v

python3 scripts/generic_split.py \
  --config databases/INDIA_UDISE_SCHOOL_PROFILES/INDIA_UDISE_SCHOOL_PROFILES/schema_config.yaml

python3 scripts/generate_samples.py \
  --database INDIA_UDISE_SCHOOL_PROFILES

python3 scripts/create_tables.py \
  --database INDIA_UDISE_SCHOOL_PROFILES \
  --databases-dir databases \
  --use-database indicdb \
  --use-schema india_udise_school_profiles \
  --create-schema

python3 scripts/load_data.py \
  --database INDIA_UDISE_SCHOOL_PROFILES \
  --databases-dir databases \
  --use-database indicdb \
  --use-schema india_udise_school_profiles
```

### Generate Database-Level Knowledge

```bash
python3 scripts/create_knowledge_file.py \
  databases/INDIA_IHDS_2005_HOUSEHOLD_SURVEY/INDIA_IHDS_2005_HOUSEHOLD_SURVEY/schema_info.md \
  --backend openrouter \
  --model deepseek/deepseek-v3.2
```

### Generate Question-Level Evidence With SEED

```bash
python3 scripts/SEED.py \
  --dataset output/INDIA_NWMP_Water_Quality_Data/task_files/INDIA_NWMP_Water_Quality_Data_text2sql_20260311_153252.jsonl \
  --output output/knowledge_files_db/INDIA_NWMP_Water_Quality_Data_evidence.json \
  --use-database indicdb \
  --use-schema india_nwmp_water_quality_data \
  --openrouter-key "$OPENROUTER_API_KEY"
```

### Translate Task Files

```bash
python3 scripts/LanguageConversionScripts/translate_to_hindi.py \
  --input_file output/INDIA_NWMP_Water_Quality_Data/task_files/INDIA_NWMP_Water_Quality_Data_text2sql_20260311_153252.jsonl \
  --target_languages "Hindi,Bengali,Tamil,Telugu,Marathi,Hinglish" \
  --provider openrouter \
  --model deepseek/deepseek-v3.2
```

### Run One-Shot Evaluation

```bash
python3 scripts/OneShot_FewShot/run_oneshot.py \
  --database INDIA_NWMP_Water_Quality_Data \
  --database-dir databases \
  --input output/INDIA_NWMP_Water_Quality_Data/task_files \
  --output output/INDIA_NWMP_Water_Quality_Data/eval_files_oneshot_qwen_qwen3_8b \
  --provider openrouter \
  --model qwen/qwen3-8b \
  --pg-db indicdb \
  --knowledge output/knowledge_files_db/INDIA_NWMP_Water_Quality_Data_evidence.json
```

### Run Few-Shot Evaluation

```bash
python3 scripts/OneShot_FewShot/run_fewshot.py \
  --database INDIA_NWMP_Water_Quality_Data \
  --database-dir databases \
  --input output/INDIA_NWMP_Water_Quality_Data/task_files/INDIA_NWMP_Water_Quality_Data_text2sql_20260311_153252.jsonl \
  --examples output/INDIA_PRIMARY_POPULATION_CENSUS_1991/task_files/INDIA_PRIMARY_POPULATION_CENSUS_1991_text2sql_20260316_104923.jsonl \
  --provider openrouter \
  --model deepseek/deepseek-v3.2 \
  --pg-db indicdb \
  --k 5
```

### Run Batch One-Shot Evaluation Across Databases

```bash
python3 scripts/BatchEvaluation/run_bulk_evaluation.py \
  --provider openrouter \
  --model minimax/minimax-m2.7 \
  --workers 20 \
  --use-lang-knowledge
```

### Recompute EM / EX Summaries

```bash
python3 scripts/BatchEvaluation/recompute_oneshot_metrics.py
```

The latest repo summary is currently written to:

- [`output/metric_summaries/oneshot_em_ex_by_db_language_model.csv`](output/metric_summaries/oneshot_em_ex_by_db_language_model.csv)
- [`output/metric_summaries/oneshot_em_ex_macro_avg_by_language_model.csv`](output/metric_summaries/oneshot_em_ex_macro_avg_by_language_model.csv)
- [`output/metric_summaries/oneshot_em_ex_macro_avg_by_language_model.md`](output/metric_summaries/oneshot_em_ex_macro_avg_by_language_model.md)

### Audit Translation Quality With COMET

```bash
python3 scripts/BatchEvaluation/calculate_comet_scores.py --batch-size 8 --gpus 0
python3 scripts/BatchEvaluation/analyze_comet_percentiles.py
```

This writes task-level COMET scores and bottom-quintile audit files under `output/comet_scores/`.

## Script Map

### Database Construction

| Script | Purpose |
| --- | --- |
| `scripts/pipeline.py` | Single entry point for design -> parse -> split -> create -> load |
| `scripts/create_schema_llm_judge.py` | LLM-based schema design using Architect/Auditor/Refiner stages |
| `scripts/parse_schema_to_yaml.py` | Converts `schema_info.md` to machine-readable `schema_config.yaml` |
| `scripts/generic_split.py` | Splits a raw CSV into normalized table CSVs using the YAML config |
| `scripts/generate_ddl.py` | Produces DDL artifacts from normalized tables |
| `scripts/generate_samples.py` | Generates Spider-style sample JSON files for each table |
| `scripts/create_tables.py` | Creates PostgreSQL tables from `DDL.csv` |
| `scripts/load_data.py` | Loads normalized CSV data into PostgreSQL |
| `scripts/sync_postgres_schema_from_yaml.py` | Sync helper for PostgreSQL schema updates |

### Task / Benchmark Generation

| Script | Purpose |
| --- | --- |
| `scripts/QueryGeneration/generate_queries.py` | Creates SQL query sets inspired by benchmark complexity levels |
| `scripts/QueryGeneration/validate_queries.py` | Executes generated SQL against PostgreSQL to validate correctness |
| `scripts/QueryGeneration/execute_queries.py` | Runs query batches and stores outputs |
| `scripts/dsqg_syn/run_synthesis.py` | Main DSQG-Syn synthetic Text-to-SQL generation entrypoint |
| `scripts/dsqg_syn/run_synthesis_batch.py` | Batch orchestration for synthetic generation |
| `scripts/dsqg_syn/sql_synthesizer.py` | SQL synthesis logic for DSQG-Syn |
| `scripts/dsqg_syn/question_generator.py` | Converts SQL into natural language questions |

### Multilingual Conversion And Evidence

| Script | Purpose |
| --- | --- |
| `scripts/LanguageConversionScripts/translate_to_hindi.py` | Multilingual task translation, including Hinglish |
| `scripts/SEED.py` | Question-level evidence generation with schema/value retrieval |
| `scripts/create_knowledge_file.py` | Database-level schema-faithful knowledge-file generation |

### Evaluation And Analysis

| Script | Purpose |
| --- | --- |
| `scripts/OneShot_FewShot/run_oneshot.py` | Parallel one-shot evaluation with EM and EX scoring |
| `scripts/OneShot_FewShot/run_fewshot.py` | Retrieval-based few-shot evaluation |
| `scripts/BatchEvaluation/run_bulk_evaluation.py` | Runs one-shot experiments over multiple databases/languages |
| `scripts/BatchEvaluation/recompute_oneshot_metrics.py` | Recomputes normalized EM and writes metric summaries |
| `scripts/sql_eval_utils.py` | SQL canonicalization and exact-match logic |
| `scripts/BatchEvaluation/calculate_comet_scores.py` | Reference-free COMET evaluation for translations |
| `scripts/BatchEvaluation/analyze_comet_percentiles.py` | Bottom-quintile translation audit workflow |
| `scripts/BatchEvaluation/plot_comet_distribution.py` | Visualization of language-specific COMET distributions |
| `scripts/BatchEvaluation/plot_comet_threshold_justification.py` | Plots thresholding analyses used in translation auditing |

### CHESS Integration

The repository also includes a `scripts/CHESS/` package for agentic SQL synthesis experiments and interoperability with IndicDB schemas.

Relevant files:

- `scripts/CHESS/src/` for the CHESS agent stack.
- `scripts/CHESS/run/` for runnable configs and shell entrypoints.
- `scripts/CHESS/tools/export_pg_schema_to_chess.py` for exporting PostgreSQL schemas into CHESS-friendly artifacts.
- `scripts/CHESS/tools/validate_chess_package.py` for package validation.

## Outputs And Stored Artifacts

The repo is already populated with experiment artifacts. Common locations:

- `databases/<DB>/<DB>/` for schema packages and normalized tables.
- `output/<DB>/task_files/` for multilingual task JSONLs.
- `output/<DB>/sampled_tasks/` for sampled evaluation subsets.
- `output/<DB>/eval_files_oneshot*/` for per-model evaluated predictions.
- `output/metric_summaries/` for aggregated EM/EX summaries.
- `output/comet_scores/` for translation quality analysis.
- `results/` and `results_*.md` for markdown summaries of model runs.

## Notes For Public Use

- Keep credential files local. The repo tracks templates, not real credentials.
- `config/postgres_credential.json`, `config/snowflake_credential.json`, and local `.env` files should stay untracked.
- Many scripts expect PostgreSQL schema names to match lowercased dataset IDs.

## Related Files

- Paper PDF: [IndicDB - Benchmarking Multilingual Text-to-SQL Capabilities in Indian Languages](./IndicDB%20-%20Benchmarking%20Multilingual%20Text-to-SQL%20Capabilities%20in%20Indian%20Languages.pdf)
- Prompt appendix: [`PROMPTS_README.md`](PROMPTS_README.md)
- Methodology notes: [`paper_content/DB_creationLogic.md`](paper_content/DB_creationLogic.md)
- Dataset stats: [`paper_content/DB_stats.md`](paper_content/DB_stats.md)

## Citation

If you use this repository, please cite the IndicDB paper linked above and describe the exact benchmark slice you used, including:

- databases included
- languages included
- evidence setting used
- evaluation metric definition used
- model checkpoints and prompting setup
