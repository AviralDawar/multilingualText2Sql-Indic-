"""
Configuration for DSQG-Syn framework.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum


class QuestionType(Enum):
    """Nine question types covering major SQL operations (from paper Table 1)."""
    BROWSE = "BrowseType"      # Scan - scanning with optional filtering
    SUMMARIZE = "SummarizeType" # Aggregate - grouping and aggregation
    REFINE = "RefineType"       # Filter - filtering with specific criteria
    ARRANGE = "ArrangeType"     # Sort - sorting by attributes
    SELECT_TOP = "SelectTopType" # TopSort - selecting top-K rows
    LINK = "LinkType"           # Join - joining tables
    EXCLUDE = "ExcludeType"     # Except - set difference
    OVERLAP = "OverlapType"     # Intersect - set intersection
    COMBINE = "CombineType"     # Union - set union


QUESTION_TYPE_DESCRIPTIONS = {
    QuestionType.BROWSE: {
        "sql_operation": "Scan",
        "description": "Questions that involve scanning all rows in a table with optional filtering",
        "example": "What are all the villages in Maharashtra with railway station access?"
    },
    QuestionType.SUMMARIZE: {
        "sql_operation": "Aggregate",
        "description": "Questions that require grouping data and performing aggregation (COUNT, SUM, AVG, etc.)",
        "example": "How many villages are there in each district?"
    },
    QuestionType.REFINE: {
        "sql_operation": "Filter",
        "description": "Questions that require filtering out rows that don't match a specific criterion",
        "example": "Which villages have ATM access within 5 km?"
    },
    QuestionType.ARRANGE: {
        "sql_operation": "Sort",
        "description": "Questions that involve sorting results based on one or more attributes",
        "example": "List districts sorted by average distance to nearest hospital."
    },
    QuestionType.SELECT_TOP: {
        "sql_operation": "TopSort",
        "description": "Questions that select the top-K rows based on certain criteria",
        "example": "What are the top 10 most populated villages in Bihar?"
    },
    QuestionType.LINK: {
        "sql_operation": "Join",
        "description": "Questions that require joining two or more tables",
        "example": "Show village names along with their state and district for villages with good road connectivity."
    },
    QuestionType.EXCLUDE: {
        "sql_operation": "Except",
        "description": "Questions that involve computing the set difference between two sets of data",
        "example": "Which villages have public bus service but not railway station access?"
    },
    QuestionType.OVERLAP: {
        "sql_operation": "Intersect",
        "description": "Questions that involve computing the intersection of two sets of data",
        "example": "Which villages have both ATM and commercial bank within the village?"
    },
    QuestionType.COMBINE: {
        "sql_operation": "Union",
        "description": "Questions that require computing the union of two sets of data",
        "example": "List all villages that have either public library or community centre access."
    }
}


@dataclass
class DSQGConfig:
    """Configuration for DSQG-Syn framework."""

    # LLM Configuration
    llm_provider: str = "openai"  # openai, anthropic, etc.
    llm_model: str = "gpt-4o-mini"  # Model to use for generation
    temperature: float = 0.7  # Global fallback temperature
    keyword_extraction_temperature: float = 0.2
    question_generation_temperature: float = 0.7
    schema_selector_temperature: float = 0.1
    skeleton_generation_temperature: float = 0.3
    sql_generation_temperature: float = 0.1
    nlq_synthesis_temperature: float = 0.4
    max_tokens: int = 2000

    # Question Generation Settings
    questions_per_table: int = 5  # Paper finds 9 is optimal (one per question type)
    max_join_depth: int = 3  # Maximum tables to join in a query

    # SQL Skeleton Settings
    skeletons_per_question: int = 2  # Paper finds 3 is optimal

    # SQL Generation Settings
    sqls_per_skeleton: int = 2  # Multiple SQLs per skeleton for diversity
    sql_difficulty: str = "mixed"  # easy, medium, hard, mixed
    sql_difficulty_mix: Dict[str, float] = field(
        default_factory=lambda: {"easy": 0.4, "medium": 0.4, "hard": 0.2}
    )

    # NLQ Semantic Optimization Settings
    top_k_nlq_pairs: int = 5  # Top K NLQ-SQL pairs to retain per original question
    similarity_threshold: float = 0.7  # Minimum similarity for filtering

    # Output Settings
    output_format: str = "jsonl"  # jsonl or json
    include_metadata: bool = True

    # Database Settings
    database_type: str = "postgresql"  # postgresql, mysql, sqlite

    # Domain Keywords (auto-extracted or manually specified)
    domain_keywords: List[str] = field(default_factory=list)


@dataclass
class SchemaInfo:
    """Information about a database schema."""
    database_name: str
    tables: Dict[str, 'TableInfo']
    foreign_keys: List[Dict]  # List of FK relationships
    domain: str = ""
    keywords: List[str] = field(default_factory=list)


@dataclass
class TableInfo:
    """Information about a single table."""
    table_name: str
    description: str
    columns: List[Dict[str, str]]  # List of {name, type, description}
    primary_key: str
    foreign_keys: List[Dict] = field(default_factory=list)
    sample_values: Dict[str, List] = field(default_factory=dict)


@dataclass
class GeneratedQuestion:
    """A domain-specific question generated in Step 1."""
    question_id: str
    question_text: str
    question_type: QuestionType
    related_tables: List[str]
    keywords: List[str]
    sql_operations: List[str]


@dataclass
class SQLSkeleton:
    """SQL skeleton structure without specific schema."""
    skeleton_id: str
    template: str  # e.g., "SELECT col_1 FROM table_1 WHERE col_2 = value_1"
    operations: List[str]
    difficulty: str = "medium"


@dataclass
class NLQSQLPair:
    """A synthesized NLQ-SQL pair."""
    pair_id: str
    original_question_id: str
    nlq: str
    sql: str
    schema_used: List[str]
    skeleton_id: str
    difficulty: str = "medium"
    similarity_score: float = 0.0
    is_valid: bool = True
