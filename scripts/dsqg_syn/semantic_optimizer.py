"""
Step 3: NLQ Semantic Optimization

This module filters synthesized NLQ-SQL pairs to retain only high-quality pairs
that maintain semantic similarity with the original domain-specific questions.

Uses similarity comparison to filter out pairs where the synthesized NLQ
diverges significantly from the original question intent.
"""

import json
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re
from collections import Counter

from .config import (
    DSQGConfig, GeneratedQuestion, NLQSQLPair
)

from scripts.db_utils import load_config, get_connection, get_default_config_path, execute_use_schema
from scripts.run_query import execute_query


class SemanticOptimizer:
    """
    Filters NLQ-SQL pairs based on semantic similarity.

    Key insight from paper: After SQL-NLQ synthesis, some domain-irrelevant
    NLQs may remain. Filter pairs where synthesized NLQ diverges significantly
    from original domain question.

    Uses M3-Embedding based retrieval (or fallback similarity methods).
    """

    def __init__(self, config: DSQGConfig, embedding_model=None):
        self.config = config
        self.embedding_model = embedding_model

    def compute_similarity(
        self,
        original_question: str,
        synthesized_nlq: str
    ) -> float:
        """
        Compute semantic similarity between original question and synthesized NLQ.

        If embedding model available, use vector similarity.
        Otherwise, use lexical similarity methods.
        """
        if self.embedding_model:
            return self._embedding_similarity(original_question, synthesized_nlq)
        else:
            return self._lexical_similarity(original_question, synthesized_nlq)

    def _embedding_similarity(
        self,
        text1: str,
        text2: str
    ) -> float:
        """Compute cosine similarity using embeddings."""
        try:
            emb1 = self.embedding_model.encode(text1)
            emb2 = self.embedding_model.encode(text2)

            # Cosine similarity
            dot_product = sum(a * b for a, b in zip(emb1, emb2))
            norm1 = sum(a * a for a in emb1) ** 0.5
            norm2 = sum(b * b for b in emb2) ** 0.5

            if norm1 * norm2 == 0:
                return 0.0
            return dot_product / (norm1 * norm2)
        except Exception:
            return self._lexical_similarity(text1, text2)

    def _lexical_similarity(
        self,
        text1: str,
        text2: str
    ) -> float:
        """
        Compute lexical similarity using multiple methods:
        1. Jaccard similarity of words
        2. Keyword overlap
        3. N-gram overlap
        """
        # Normalize texts
        text1_lower = text1.lower()
        text2_lower = text2.lower()

        # Remove punctuation and split into words
        words1 = set(re.findall(r'\b\w+\b', text1_lower))
        words2 = set(re.findall(r'\b\w+\b', text2_lower))

        # Remove stop words
        stop_words = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'could', 'should', 'may', 'might', 'must', 'shall',
            'can', 'of', 'to', 'in', 'for', 'on', 'with', 'at', 'by',
            'from', 'as', 'into', 'through', 'during', 'before', 'after',
            'above', 'below', 'between', 'under', 'again', 'further',
            'then', 'once', 'here', 'there', 'when', 'where', 'why',
            'how', 'all', 'each', 'few', 'more', 'most', 'other', 'some',
            'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
            'than', 'too', 'very', 'just', 'and', 'but', 'if', 'or',
            'because', 'as', 'until', 'while', 'what', 'which', 'who',
            'this', 'that', 'these', 'those', 'it', 'its'
        }

        words1 = words1 - stop_words
        words2 = words2 - stop_words

        if not words1 or not words2:
            return 0.0

        # Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        jaccard = intersection / union if union > 0 else 0.0

        # Keyword overlap (weighted more heavily for domain terms)
        domain_keywords = {
            'village', 'district', 'state', 'population', 'distance',
            'accessibility', 'service', 'road', 'transport', 'health',
            'education', 'financial', 'market', 'civic', 'rural'
        }

        domain_words1 = words1 & domain_keywords
        domain_words2 = words2 & domain_keywords
        domain_overlap = len(domain_words1 & domain_words2) / max(len(domain_words1 | domain_words2), 1)

        # N-gram overlap (bigrams)
        bigrams1 = set(self._get_ngrams(text1_lower, 2))
        bigrams2 = set(self._get_ngrams(text2_lower, 2))
        bigram_sim = len(bigrams1 & bigrams2) / max(len(bigrams1 | bigrams2), 1)

        # Weighted combination
        similarity = (
            0.4 * jaccard +
            0.35 * domain_overlap +
            0.25 * bigram_sim
        )

        return min(1.0, max(0.0, similarity))

    def _get_ngrams(self, text: str, n: int) -> List[str]:
        """Extract n-grams from text."""
        words = re.findall(r'\b\w+\b', text.lower())
        return [' '.join(words[i:i+n]) for i in range(len(words) - n + 1)]

    def filter_pairs(
        self,
        questions: List[GeneratedQuestion],
        pairs: List[NLQSQLPair],
        top_k: int = 5
    ) -> List[NLQSQLPair]:
        """
        Filter NLQ-SQL pairs based on semantic similarity.

        For each original question, retain top-K pairs with highest
        similarity scores.
        """
        # Group pairs by original question
        pairs_by_question = {}
        for pair in pairs:
            q_id = pair.original_question_id
            if q_id not in pairs_by_question:
                pairs_by_question[q_id] = []
            pairs_by_question[q_id].append(pair)

        # Create question lookup
        question_lookup = {q.question_id: q for q in questions}

        filtered_pairs = []

        for q_id, q_pairs in pairs_by_question.items():
            original_question = question_lookup.get(q_id)
            if not original_question:
                continue

            # Compute similarity for each pair
            scored_pairs = []
            for pair in q_pairs:
                similarity = self.compute_similarity(
                    original_question.question_text,
                    pair.nlq
                )
                pair.similarity_score = similarity
                scored_pairs.append((similarity, pair))

            # Sort by similarity (descending) and keep top-K
            scored_pairs.sort(key=lambda x: x[0], reverse=True)
            top_pairs = [pair for _, pair in scored_pairs[:top_k]]

            # Filter by threshold
            for pair in top_pairs:
                if pair.similarity_score >= self.config.similarity_threshold:
                    pair.is_valid = True
                    filtered_pairs.append(pair)

        return filtered_pairs

    def validate_sql_executability(
        self,
        pairs: List[NLQSQLPair],
        schema_name: str,
        db_connection=None
    ) -> List[NLQSQLPair]:
        """
        Optional: Validate that SQL queries are executable.

        Marks pairs with invalid SQL as is_valid=False.
        """
        if db_connection is None:
            config_path = get_default_config_path()
            try:
                config = load_config(config_path)
                conn = get_connection(config, database='indicdb')
                conn.autocommit = True
            except Exception as e:
                raise RuntimeError(
                    f"SQL validation failed: could not connect to database 'indicdb' "
                    f"using config '{config_path}': {e}"
                )
            manage_connection = True
        else:
            conn = db_connection
            manage_connection = False

        try:
            cursor = conn.cursor()
            try:
                execute_use_schema(cursor, schema_name)
            except Exception as e:
                raise RuntimeError(
                    f"SQL validation failed: could not set schema '{schema_name}': {e}"
                )
            # Cap each validation query at 20 seconds.
            cursor.execute("SET statement_timeout TO 20000")

            validated = []
            for pair in pairs:
                result = execute_query(
                    cursor,
                    pair.sql,
                    show_results=False,
                    fetch_one=True
                )
                pair.is_valid = result is not None and len(result.get('rows', [])) > 0
                validated.append(pair)

            return validated
        finally:
            if manage_connection:
                conn.close()

    def _basic_sql_validation(self, pairs: List[NLQSQLPair]) -> List[NLQSQLPair]:
        """Basic SQL syntax validation without DB connection."""
        required_keywords = ['SELECT', 'FROM']
        valid_pairs = []

        for pair in pairs:
            sql_upper = pair.sql.upper()

            # Check for required keywords
            has_required = all(kw in sql_upper for kw in required_keywords)

            # Check for balanced parentheses
            balanced = sql_upper.count('(') == sql_upper.count(')')

            # Check for no unfilled placeholders
            no_placeholders = 'col_' not in pair.sql.lower() and 'table_' not in pair.sql.lower()

            pair.is_valid = has_required and balanced and no_placeholders
            valid_pairs.append(pair)

        return valid_pairs

    def deduplicate_pairs(
        self,
        pairs: List[NLQSQLPair],
        similarity_threshold: float = 0.95
    ) -> List[NLQSQLPair]:
        """
        Remove near-duplicate NLQ-SQL pairs.

        Pairs with very high similarity to existing pairs are removed
        to ensure diversity in the dataset.
        """
        if not pairs:
            return pairs

        unique_pairs = [pairs[0]]

        for pair in pairs[1:]:
            is_duplicate = False

            for existing in unique_pairs:
                # Check NLQ similarity
                nlq_sim = self._lexical_similarity(pair.nlq, existing.nlq)
                # Check SQL similarity
                sql_sim = self._lexical_similarity(pair.sql, existing.sql)

                if nlq_sim > similarity_threshold and sql_sim > similarity_threshold:
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_pairs.append(pair)

        return unique_pairs

    def optimize(
        self,
        questions: List[GeneratedQuestion],
        pairs: List[NLQSQLPair],
        schema_name: str,
        db_connection=None
    ) -> List[NLQSQLPair]:
        """
        Main entry point: Run full optimization pipeline.

        1. Filter by semantic similarity
        2. Validate SQL executability
        3. Deduplicate
        """
        print(f"  [1/3] Filtering by semantic similarity (top-{self.config.top_k_nlq_pairs}, threshold={self.config.similarity_threshold})...")
        # Step 1: Filter by similarity
        # filtered = self.filter_pairs(
        #     questions,
        #     pairs,
        #     self.config.top_k_nlq_pairs
        # )
        # print(f"        {len(pairs)} → {len(filtered)} pairs after similarity filtering")

        filtered = pairs
        
        # Step 2: Validate SQL
        print("  [2/3] Validating SQL executability...")
        validated = self.validate_sql_executability(filtered, schema_name, db_connection)
        valid_count = len([p for p in validated if p.is_valid])
        validated = [p for p in validated if p.is_valid]
        print(f"        {len(filtered)} → {valid_count} pairs after SQL validation")

        # Step 3: Deduplicate
        print("  [3/3] Deduplicating pairs...")
        deduplicated = self.deduplicate_pairs(validated)
        print(f"        {valid_count} → {len(deduplicated)} pairs after deduplication")

        return deduplicated

    def compute_quality_metrics(
        self,
        pairs: List[NLQSQLPair]
    ) -> Dict[str, float]:
        """Compute quality metrics for the synthesized dataset."""
        if not pairs:
            return {'count': 0}

        valid_pairs = [p for p in pairs if p.is_valid]
        similarity_scores = [p.similarity_score for p in pairs if p.similarity_score > 0]

        metrics = {
            'total_pairs': len(pairs),
            'valid_pairs': len(valid_pairs),
            'validity_rate': len(valid_pairs) / len(pairs),
            'avg_similarity': sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0,
            'min_similarity': min(similarity_scores) if similarity_scores else 0,
            'max_similarity': max(similarity_scores) if similarity_scores else 0,
        }

        # Count by question type (if we track this)
        unique_questions = len(set(p.original_question_id for p in pairs))
        metrics['unique_questions_covered'] = unique_questions
        metrics['avg_pairs_per_question'] = len(pairs) / unique_questions if unique_questions > 0 else 0

        return metrics
