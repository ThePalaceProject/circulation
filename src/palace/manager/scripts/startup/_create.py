"""Scaffold a new startup task file.

Prefixed with ``_`` so that :func:`discover_startup_tasks` skips it during
auto-discovery.
"""

from __future__ import annotations

import re
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

STARTUP_TASKS_DIR = Path(__file__).parents[5] / "startup_tasks"

TEMPLATE = '''\
"""{description}"""

from __future__ import annotations

from celery.canvas import Signature


def create_signature() -> Signature:
    """Build the Celery signature to dispatch.

    Uses a local import to avoid import-time coupling with the Celery app,
    which may not be configured when the init script first imports this package.
    """
    raise NotImplementedError("TODO: return a Celery signature here")
'''


def _slugify(text: str) -> str:
    """Convert a description into a valid Python identifier slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug


def main() -> None:
    parser = ArgumentParser(
        description="Create a new startup task file.",
    )
    parser.add_argument(
        "description",
        help='Short description, e.g. "force harvest opds for distributors"',
    )
    parser.add_argument(
        "--date-prefix",
        default=None,
        help="Override the YYYY_MM_DD_HHMM date prefix (default: current UTC time).",
    )
    args = parser.parse_args()

    description: str = args.description
    if args.date_prefix:
        date_prefix = args.date_prefix
    else:
        now = datetime.now(tz=timezone.utc)
        date_prefix = now.strftime("%Y_%m_%d_%H%M")

    slug = _slugify(description)
    if not slug:
        print("Error: description must contain at least one alphanumeric character.")
        sys.exit(1)

    filename = f"{date_prefix}_{slug}.py"
    filepath = STARTUP_TASKS_DIR / filename

    if filepath.exists():
        print(f"Error: {filepath} already exists.")
        sys.exit(1)

    content = TEMPLATE.format(description=description)
    filepath.write_text(content)
    print(f"Created {filepath.relative_to(Path.cwd())}")
    print(f"Task key will be: {filepath.stem}")
    print()
    print("Next steps:")
    print("  1. Implement create_signature() in the generated file.")
    print("  2. Commit and deploy.")


if __name__ == "__main__":
    main()
