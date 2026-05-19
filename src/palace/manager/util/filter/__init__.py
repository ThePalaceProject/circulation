"""Generic filter expression evaluation.

Provides a safe, eval-free DSL for evaluating boolean expressions over a
named context dictionary. Domain-specific wiring (context contents and
safe types) is the caller's responsibility.
"""

from palace.manager.util.filter.expression import (
    FilterExpression,
    FilterExpressionError,
)

__all__ = ["FilterExpression", "FilterExpressionError"]
