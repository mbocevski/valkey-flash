#!/usr/bin/env python3
"""
Generate Conventional Commits-grouped release notes for a git tag.

Usage:
  python3 scripts/release-notes.py <tag>

Outputs markdown to stdout. Intended for use in the release CI workflow.
"""

import re
import subprocess
import sys

TYPE_ORDER = ["feat", "fix", "perf", "refactor", "docs", "test", "ci", "build", "chore"]
TYPE_LABELS = {
    "feat":     "Features",
    "fix":      "Bug Fixes",
    "perf":     "Performance",
    "refactor": "Refactoring",
    "docs":     "Documentation",
    "test":     "Tests",
    "ci":       "CI/CD",
    "build":    "Build",
    "chore":    "Chores",
}

CC_RE = re.compile(r'^(\w+)(?:\([\w/.\-]+\))?(!)?:\s+(.+)$')


def git(*args: str) -> str:
    return subprocess.check_output(["git"] + list(args), text=True).strip()


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: release-notes.py <tag>", file=sys.stderr)
        sys.exit(1)

    tag = sys.argv[1]

    # Find the previous tag (chronologically before this one)
    all_tags = [t for t in git("tag", "--sort=-creatordate").splitlines() if t]
    prev_tags = [t for t in all_tags if t != tag]
    prev_tag = prev_tags[0] if prev_tags else None

    # Collect commit subjects since the previous tag (or all history if first tag)
    range_spec = f"{prev_tag}..HEAD" if prev_tag else "HEAD"
    raw = git("log", "--pretty=format:%s", range_spec)
    subjects = [s for s in raw.splitlines() if s.strip()]

    # Parse and bucket by Conventional Commits type
    groups: dict = {t: [] for t in TYPE_ORDER}
    breaking: list = []
    other: list = []

    for subject in subjects:
        m = CC_RE.match(subject)
        if m:
            typ, is_breaking, desc = m.group(1), m.group(2), m.group(3)
            if is_breaking:
                breaking.append(desc)
            if typ in groups:
                groups[typ].append(desc)
            elif not is_breaking:
                other.append(subject)
        elif subject:
            other.append(subject)

    # Render markdown
    lines: list = []

    if prev_tag:
        lines.append(f"Changes since {prev_tag}.\n")

    if breaking:
        lines.append("### Breaking Changes\n")
        for item in breaking:
            lines.append(f"- {item}")
        lines.append("")

    for typ in TYPE_ORDER:
        if groups[typ]:
            lines.append(f"### {TYPE_LABELS[typ]}\n")
            for item in groups[typ]:
                lines.append(f"- {item}")
            lines.append("")

    if other:
        lines.append("### Other\n")
        for item in other:
            lines.append(f"- {item}")
        lines.append("")

    all_items = breaking + [i for g in groups.values() for i in g] + other
    if not all_items:
        lines.append("No user-facing changes.")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
