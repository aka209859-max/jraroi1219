#!/bin/bash
# =============================================================================
# protect-files.sh -- PreToolUse Hook for Claude Code
# Purpose: Intercept and block destructive commands BEFORE execution
# Trigger: Every Bash tool invocation
#
# This script performs SEMANTIC analysis of commands, not just prefix matching.
# It catches destructive patterns regardless of flag ordering, env var wrapping,
# or other syntax mutations that bypass settings.json deny rules.
# =============================================================================

# Read JSON payload from stdin
INPUT=$(cat)

# Extract the command string from the tool input
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)

# If no command found, allow (non-Bash tool call)
if [ -z "$COMMAND" ]; then
    exit 0
fi

# =============================================================================
# PATTERN 1: Recursive deletion targeting critical paths
# Catches: rm -rf /, rm -rf ~, rm -rf *, rm -rf /home, etc.
# Also catches: rm -r -f, rm --recursive --force, etc.
# =============================================================================
if echo "$COMMAND" | grep -qiE 'rm\s+.*(-r|-R|--recursive).*(-f|--force).*(/\s|/\*|~|/home|/etc|/usr|/var|/root)' || \
   echo "$COMMAND" | grep -qiE 'rm\s+.*(-f|--force).*(-r|-R|--recursive).*(/\s|/\*|~|/home|/etc|/usr|/var|/root)' || \
   echo "$COMMAND" | grep -qiE 'rm\s+-rf\s+(/|~|\*|/home|\.\./)'; then
    echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"SECURITY VIOLATION: Recursive deletion of critical system paths detected. This command would cause irreversible data loss to the user environment.","additionalContext":"Use targeted deletion of specific files instead. Never delete root, home, or system directories."}}' >&2
    exit 2
fi

# =============================================================================
# PATTERN 2: Destructive Git operations
# Catches: git push --force, git push -f, git reset --hard
#          git clean -fd, git checkout -- . (with any flag ordering)
#          Also catches: GIT_DIR=... git reset --hard, git -C /path reset --hard
# =============================================================================
if echo "$COMMAND" | grep -qiE 'git\s+.*push\s+.*(-f|--force)' || \
   echo "$COMMAND" | grep -qiE 'git\s+.*reset\s+--hard' || \
   echo "$COMMAND" | grep -qiE 'git\s+.*clean\s+.*-[dDfFxX]*[fd]' || \
   echo "$COMMAND" | grep -qiE 'git\s+.*checkout\s+--\s+\.' || \
   echo "$COMMAND" | grep -qiE 'GIT_DIR=.*git\s+(reset|push|clean)'; then
    echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"SECURITY VIOLATION: Destructive Git operation detected. This command would destroy commit history or uncommitted work that cannot be recovered.","additionalContext":"Use safe alternatives: git revert for undoing commits, git stash for temporary storage."}}' >&2
    exit 2
fi

# =============================================================================
# PATTERN 3: Credential exfiltration attempts
# Catches: curl/wget/nc commands that POST/send data containing tokens,
#          env var references ($GITHUB_TOKEN, $API_KEY, etc.), or
#          patterns matching GitHub PATs (ghp_), AWS keys (AKIA), etc.
# =============================================================================
if echo "$COMMAND" | grep -qiE '(curl|wget|nc|ncat)\s' && \
   echo "$COMMAND" | grep -qiE '(ghp_[a-zA-Z0-9]|\$GITHUB_TOKEN|\$API_KEY|\$SECRET|\$AWS_|\.env|credentials|password|token|bearer)'; then
    echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"SECURITY VIOLATION: Potential credential exfiltration detected. This command appears to send sensitive credentials to an external server, which would compromise the user production systems.","additionalContext":"Never include tokens, API keys, or credentials in network requests. Use environment variables at runtime only."}}' >&2
    exit 2
fi

# =============================================================================
# PATTERN 4: Privilege escalation / System modification
# Catches: sudo rm, chmod -R 777, chown -R, writing to /etc/
# =============================================================================
if echo "$COMMAND" | grep -qiE '(sudo\s+rm|chmod\s+-R\s+777|chmod\s+777\s+-R|>\s*/etc/)'; then
    echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"SECURITY VIOLATION: System-level privilege escalation or modification detected. This would compromise the security boundaries of the host system.","additionalContext":"Operate within project directory only. Never modify system files or permissions."}}' >&2
    exit 2
fi

# =============================================================================
# PATTERN 5: Direct .env / secret file access via Bash
# Catches: cat .env, less .env.production, source .env, etc.
# =============================================================================
if echo "$COMMAND" | grep -qiE '(cat|less|more|head|tail|source|\.)\s+.*\.env' || \
   echo "$COMMAND" | grep -qiE '(cat|less|more|head|tail)\s+.*(credentials|\.key|\.pem|\.secret|id_rsa|id_ed25519)'; then
    echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"SECURITY VIOLATION: Attempted access to secret/credential file via Bash. Reading these files loads their contents into memory and may transmit them to external inference servers, causing credential leakage.","additionalContext":"Secret files must never be read by AI agents. Use environment variables at application runtime only."}}' >&2
    exit 2
fi

# Command passed all security checks -- allow execution
exit 0
