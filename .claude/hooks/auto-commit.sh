#!/bin/bash
# =============================================================================
# auto-commit.sh -- Stop Hook for Claude Code
# Purpose: Automatically create a Git commit after every Claude Code response
# Trigger: Every time Claude Code finishes responding (Stop event)
#
# This hook compensates for the critical limitation of Claude Code's checkpoint
# system: /rewind CANNOT track changes made via Bash tool (rm, mv, sed, etc.).
# By auto-committing after every turn, git reflog becomes the ultimate safety net.
# =============================================================================

# 1. Verify we're inside a Git repository
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    exit 0
fi

# 2. Check if there are any uncommitted changes (staged, unstaged, or untracked)
if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
    # No changes to commit
    exit 0
fi

# 3. Read the JSON payload from stdin and extract session info
INPUT=$(cat)
SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // "unknown"' 2>/dev/null)
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# 4. Get a summary of changed files (max 5 for commit message brevity)
CHANGED_FILES=$(git status --short | head -5 | tr '\n' ' ')
TOTAL_CHANGES=$(git status --short | wc -l | tr -d ' ')

# 5. Stage all changes and create auto-commit
git add -A > /dev/null 2>&1
git commit -m "auto-commit by Claude Code | ${TIMESTAMP} | ${TOTAL_CHANGES} file(s) | ${CHANGED_FILES}" \
    --no-verify > /dev/null 2>&1

# 6. Return silent success (suppress output in Claude Code UI)
echo '{"systemMessage": "Auto-committed changes to Git for safety", "suppressOutput": true}'
exit 0
