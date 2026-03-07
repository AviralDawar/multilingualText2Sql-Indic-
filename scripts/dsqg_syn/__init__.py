"""
DSQG-Syn: Domain-Specific Question Generation for Text-to-SQL Synthesis

Implementation based on the paper:
"DSQG-Syn: Synthesizing High-quality Data for Text-to-SQL Parsing by Domain Specific Question Generation"
(Duan et al., NAACL 2025)

This framework generates high-quality NLQ-SQL pairs through:
1. Domain-specific question generation
2. Question-guided SQL-NLQ synthesis
3. NLQ semantic optimization
"""

from .dsqg_syn import DSQGSyn
from .question_generator import DomainQuestionGenerator
from .sql_synthesizer import SQLNLQSynthesizer
from .semantic_optimizer import SemanticOptimizer

__all__ = [
    'DSQGSyn',
    'DomainQuestionGenerator',
    'SQLNLQSynthesizer',
    'SemanticOptimizer'
]
