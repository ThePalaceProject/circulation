Post line-specific findings as inline comments via
`mcp__github_inline_comment__create_inline_comment` (with `confirmed: true`) —
do not collapse them into the summary.

### Bug-flagging criteria

- It meaningfully impacts the accuracy, performance, security, or maintainability of the code.
- The bug is discrete and actionable (i.e. not a general issue with the codebase or a combination of multiple issues).
- The bug was introduced in the commit (pre-existing bugs should not be flagged).
- The author of the original PR would likely fix the issue if they were made aware of it.
- It is not enough to speculate that a change may disrupt another part of the codebase, to be considered a bug,
  one must identify the other parts of the code that are provably affected.
- The bug is clearly not just an intentional change by the original author.

### Comment quality

- The comment should be clear about why the issue is a bug.
- The comment should appropriately communicate the severity of the issue. It should not claim that an issue is
  more severe than it actually is.
- The comment should be brief. The body should be at most 1 paragraph. It should not introduce line breaks
  within the natural language flow unless it is necessary for the code fragment.
- The comment should clearly and explicitly communicate the scenarios, environments, or inputs that are
  necessary for the bug to arise. The comment should immediately indicate that the issue's severity depends
  on these factors.
- The comment should be written such that the original author can immediately grasp the idea without close
  reading.

### Severity prefixes

Findings that pass the bug-flagging criteria above are unprefixed; weaker concerns are `Minor:` or
`Nit:`.

- `Nit:` — subjective / personal preference; use sparingly.
- `Minor:` — real concern but not a bug or behavior change. CLAUDE.md convention violations typically
  fall into this category.
- No prefix — findings that meet the bug-flagging criteria above.

Skip formatting and style findings entirely — pre-commit handles those.

The comments will be presented in the code review as inline comments. You should avoid providing
unnecessary location details in the comment body. Always keep the line range as short as possible for
interpreting the issue. Avoid ranges longer than 5–10 lines; instead, choose the most suitable subrange
that pinpoints the problem.

Your summary is the OVERALL summary, not a restatement of the inline comments:
give a high-level take and flag cross-cutting concerns only. DO NOT repeat anything
already covered inline. It should be succinct, one paragraph maximum.
