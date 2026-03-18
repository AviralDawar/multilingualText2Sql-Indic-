#!/usr/bin/env python3
"""
Run DSQG-Syn Text-to-SQL Data Synthesis

This script runs the complete DSQG-Syn pipeline to generate NLQ-SQL pairs
for a given database schema.

Usage:
    # With OpenRouter (default)
    python -m scripts.dsqg_syn.run_synthesis --schema path/to/schema.yaml --model deepseek/deepseek-chat

    # With specific API key
    python -m scripts.dsqg_syn.run_synthesis --schema path/to/schema.yaml --model deepseek/deepseek-chat --api-key YOUR_KEY

Note: Requires OPENROUTER_API_KEY environment variable or --api-key argument.
"""

import argparse
import json
import os
import sys
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.dsqg_syn.config import DSQGConfig
from scripts.dsqg_syn.dsqg_syn import DSQGSyn


class OpenRouterLLM:
    """OpenRouter API client for various LLM models."""

    def __init__(self, model: str = "deepseek/deepseek-chat", api_key: str = None,
                 temperature: float = 0.7, max_tokens: int = 2000):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found. Set it via --api-key or environment variable.")
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.request_timeout = (15, 180)  # (connect timeout, read timeout)

    def _make_request(
        self,
        messages: list,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> dict:
        """Make API request and return the full message object."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature if temperature is None else temperature,
            "max_tokens": self.max_tokens if max_tokens is None else max_tokens,
        }

        response = requests.post(
            self.base_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=self.request_timeout
        )

        if response.status_code != 200:
            raise RuntimeError(f"OpenRouter API error: {response.status_code} - {response.text}")

        return response.json()['choices'][0]['message']

    def generate(
        self,
        prompt: str,
        max_tokens: int = None,
        temperature: float = None
    ) -> str:
        """Send a prompt and return response content (stateless call)."""
        messages = [
            {"role": "system", "content": "You are a helpful assistant specialized in databases and SQL."},
            {"role": "user", "content": prompt}
        ]

        try:
            response_msg = self._make_request(
                messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response_msg.get('content', '')
        except Exception as e:
            print(f"LLM generation error: {e}")
            return ""


class GeminiLLM:
    """Gemini SDK client (google-generativeai) with same generate() shape."""

    def __init__(
        self,
        model: str = "gemini-2.0-flash",
        api_key: str = None,
        temperature: float = 0.7,
        max_tokens: int = 2000
    ):
        self.model_name = model
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found. Set it via --gemini-api-key or environment variable.")

        try:
            import google.generativeai as genai
        except ImportError as e:
            raise ValueError(
                "Gemini SDK is not installed. Install with: pip install google-generativeai"
            ) from e

        genai.configure(api_key=self.api_key)
        self._model = genai.GenerativeModel(self.model_name)
        self.temperature = temperature
        self.max_tokens = max_tokens

    def generate(
        self,
        prompt: str,
        max_tokens: int = None,
        temperature: float = None
    ) -> str:
        config = {
            "temperature": self.temperature if temperature is None else temperature,
            "max_output_tokens": self.max_tokens if max_tokens is None else max_tokens,
        }
        try:
            response = self._model.generate_content(
                prompt,
                generation_config=config
            )
            text = getattr(response, "text", "")
            return text or ""
        except Exception as e:
            print(f"Gemini generation error: {e}")
            return ""


def main():
    parser = argparse.ArgumentParser(
        description='Run DSQG-Syn Text-to-SQL Data Synthesis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Required arguments
    parser.add_argument(
        '--schema',
        type=str,
        required=True,
        help='Path to schema configuration file (YAML or DDL CSV)'
    )

    # Output arguments
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output file path (default: output/<database>_text2sql_<timestamp>.jsonl)'
    )
    parser.add_argument(
        '--output-format',
        type=str,
        choices=['jsonl', 'json'],
        default='jsonl',
        help='Output format'
    )

    # LLM arguments
    parser.add_argument(
        '--model',
        type=str,
        default='deepseek/deepseek-chat',
        help='OpenRouter model to use (default: deepseek/deepseek-chat)'
    )
    parser.add_argument(
        '--api-key',
        type=str,
        default=None,
        help='OpenRouter API key (or set OPENROUTER_API_KEY environment variable)'
    )
    parser.add_argument(
        '--temperature',
        type=float,
        default=0.7,
        help='LLM temperature (default: 0.7)'
    )
    parser.add_argument(
        '--max-tokens',
        type=int,
        default=2000,
        help='Max tokens per LLM response (default: 2000)'
    )
    parser.add_argument(
        '--use-gemini-for-sql-nlq',
        action='store_true',
        help='Use Gemini SDK for the entire pipeline'
    )
    parser.add_argument(
        '--gemini-model',
        type=str,
        default='gemini-2.0-flash',
        help='Gemini model for SQL/NLQ stages (default: gemini-2.0-flash)'
    )
    parser.add_argument(
        '--gemini-api-key',
        type=str,
        default=None,
        help='Gemini API key (or set GEMINI_API_KEY environment variable)'
    )
    parser.add_argument(
        '--temp-keyword-extraction',
        type=float,
        default=None,
        help='Temperature for keyword extraction stage'
    )
    parser.add_argument(
        '--temp-question-generation',
        type=float,
        default=None,
        help='Temperature for question generation stage'
    )
    parser.add_argument(
        '--temp-schema-selector',
        type=float,
        default=None,
        help='Temperature for schema selector stage'
    )
    parser.add_argument(
        '--temp-skeleton-generation',
        type=float,
        default=None,
        help='Temperature for SQL skeleton generation stage'
    )
    parser.add_argument(
        '--temp-sql-generation',
        type=float,
        default=None,
        help='Temperature for SQL generation-from-skeleton stage'
    )
    parser.add_argument(
        '--temp-nlq-synthesis',
        type=float,
        default=None,
        help='Temperature for NLQ synthesis-from-SQL stage'
    )

    # Synthesis parameters
    parser.add_argument(
        '--questions-per-table',
        type=int,
        default=9,
        help='Number of questions to generate per table (default: 9)'
    )
    parser.add_argument(
        '--skeletons-per-question',
        type=int,
        default=2,
        help='Number of SQL skeletons per question (default: 4)'
    )
    parser.add_argument(
        '--sqls-per-skeleton',
        type=int,
        default=1,
        help='Number of SQLs per skeleton (default: 1)'
    )
    parser.add_argument(
        '--sql-difficulty',
        type=str,
        choices=['easy', 'medium', 'hard', 'mixed'],
        default='mixed',
        help='Target SQL difficulty mode for skeleton/SQL generation (default: mixed)'
    )
    parser.add_argument(
        '--sql-difficulty-mix',
        type=str,
        default='0.30,0.40,0.30',
        help='Difficulty mix for mixed mode as easy,medium,hard ratios (default: 0.25,0.5,0.25)'
    )
    parser.add_argument(
        '--top-k',
        type=int,
        default=5,
        help='Top K pairs to retain per original question (default: 5)'
    )
    parser.add_argument(
        '--similarity-threshold',
        type=float,
        default=0.3,
        help='Minimum similarity threshold for filtering (default: 0.3)'
    )

    # Data path for schema linking with sample values
    parser.add_argument(
        '--data-path',
        type=str,
        default=None,
        help='Path to database directory containing CSV/JSON data files for sample value extraction in schema linking'
    )

    # Debug arguments
    parser.add_argument(
        '--export-questions',
        action='store_true',
        help='Also export generated questions to a separate file'
    )
    parser.add_argument(
        '--export-report',
        action='store_true',
        help='Also export synthesis report'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print verbose output'
    )

    args = parser.parse_args()

    # Parse SQL difficulty mix
    try:
        mix_parts = [float(v.strip()) for v in args.sql_difficulty_mix.split(',')]
        if len(mix_parts) != 3:
            raise ValueError("expected 3 comma-separated values")
        easy_ratio, medium_ratio, hard_ratio = mix_parts
        if min(mix_parts) < 0:
            raise ValueError("ratios must be non-negative")
        if easy_ratio + medium_ratio + hard_ratio == 0:
            raise ValueError("sum of ratios must be > 0")
        sql_difficulty_mix = {
            'easy': easy_ratio,
            'medium': medium_ratio,
            'hard': hard_ratio
        }
    except Exception as e:
        print(f"Error: invalid --sql-difficulty-mix '{args.sql_difficulty_mix}': {e}")
        sys.exit(1)

    # Validate schema path
    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        sys.exit(1)

    # Initialize configuration
    config = DSQGConfig(
        questions_per_table=args.questions_per_table,
        skeletons_per_question=args.skeletons_per_question,
        sqls_per_skeleton=args.sqls_per_skeleton,
        sql_difficulty=args.sql_difficulty,
        sql_difficulty_mix=sql_difficulty_mix,
        top_k_nlq_pairs=args.top_k,
        similarity_threshold=args.similarity_threshold,
        output_format=args.output_format,
        temperature=args.temperature,
        keyword_extraction_temperature=(
            args.temp_keyword_extraction
            if args.temp_keyword_extraction is not None
            else DSQGConfig.keyword_extraction_temperature
        ),
        question_generation_temperature=(
            args.temp_question_generation
            if args.temp_question_generation is not None
            else DSQGConfig.question_generation_temperature
        ),
        schema_selector_temperature=(
            args.temp_schema_selector
            if args.temp_schema_selector is not None
            else DSQGConfig.schema_selector_temperature
        ),
        skeleton_generation_temperature=(
            args.temp_skeleton_generation
            if args.temp_skeleton_generation is not None
            else DSQGConfig.skeleton_generation_temperature
        ),
        sql_generation_temperature=(
            args.temp_sql_generation
            if args.temp_sql_generation is not None
            else DSQGConfig.sql_generation_temperature
        ),
        nlq_synthesis_temperature=(
            args.temp_nlq_synthesis
            if args.temp_nlq_synthesis is not None
            else DSQGConfig.nlq_synthesis_temperature
        )
    )

    # Initialize one LLM client for the full pipeline
    if args.use_gemini_for_sql_nlq:
        try:
            llm_client = GeminiLLM(
                model=args.gemini_model,
                api_key=args.gemini_api_key,
                temperature=args.temperature,
                max_tokens=args.max_tokens
            )
            print(f"Using Gemini for full pipeline: {args.gemini_model} (temperature={args.temperature}, max_tokens={args.max_tokens})")
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    else:
        try:
            llm_client = OpenRouterLLM(
                model=args.model,
                api_key=args.api_key,
                temperature=args.temperature,
                max_tokens=args.max_tokens
            )
            print(f"Using OpenRouter for full pipeline: {args.model} (temperature={args.temperature}, max_tokens={args.max_tokens})")
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

    # Initialize DSQG-Syn
    if args.data_path:
        print(f"Schema linking will use sample values from: {args.data_path}")
    dsqg = DSQGSyn(
        config,
        llm_client=llm_client,
        data_path=args.data_path
    )

    # Load schema
    print(f"Loading schema from: {schema_path}")
    if schema_path.suffix == '.yaml':
        schema = dsqg.load_schema_from_yaml(str(schema_path))
    elif schema_path.suffix == '.csv' and 'DDL' in schema_path.name:
        schema = dsqg.load_schema_from_ddl(str(schema_path))
    else:
        print(f"Error: Unsupported schema file format: {schema_path.suffix}")
        sys.exit(1)

    print(f"Loaded schema: {schema.database_name}")
    print(f"Tables: {list(schema.tables.keys())}")
    print(f"FKs: {list(schema.foreign_keys)}")

    # Run synthesis
    pairs = dsqg.synthesize(schema)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = project_root / 'output'
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = output_dir / f"{schema.database_name}_text2sql_{timestamp}.{args.output_format}"

    # Export results
    if args.output_format == 'jsonl':
        dsqg.export_to_jsonl(pairs, str(output_path))
    else:
        dsqg.export_to_json(pairs, str(output_path))

    # Export questions if requested
    if args.export_questions:
        questions_path = output_path.with_suffix('.questions.json')
        dsqg.export_questions(str(questions_path))

    # Export report if requested
    if args.export_report:
        report = dsqg.get_synthesis_report()
        report_path = output_path.with_suffix('.report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"Report exported to: {report_path}")

    print(f"\n=== Synthesis Complete ===")
    print(f"Output: {output_path}")
    print(f"Total pairs: {len(pairs)}")

    # Print sample pairs
    if args.verbose and pairs:
        print("\n=== Sample NLQ-SQL Pairs ===")
        for i, pair in enumerate(pairs[:5]):
            print(f"\n[{i+1}] Question: {pair.nlq}")
            print(f"    SQL: {pair.sql}")
            print(f"    Similarity: {pair.similarity_score:.3f}")


if __name__ == '__main__':
    main()
