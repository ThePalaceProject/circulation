Post line-specific findings as inline comments via
`mcp__github_inline_comment__create_inline_comment` (with `confirmed: true`) —
do not collapse them into the sticky summary.

Prefix each inline comment by severity:

- `Nit:` — subjective / personal preference; use sparingly.
- `Minor:` — real concern but not a bug or behavior change. CLAUDE.md convention violations typically
  fall into this category.
- No prefix — Anything more serious that doesn't fall into the other two severity buckets.

Skip formatting and style findings entirely — pre-commit handles those.

Your sticky review comment is the OVERALL summary, not a restatement of the
inline comments: give a high-level take and flag cross-cutting concerns only.
Do not repeat anything already covered inline. It should be succinct, one or two
paragraphs maximum.
