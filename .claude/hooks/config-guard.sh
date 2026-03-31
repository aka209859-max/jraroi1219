#!/bin/bash
# =============================================================================
# config-guard.sh -- ConfigChange Hook for Claude Code
# Purpose: Monitor and BLOCK unauthorized changes to security configuration files
# Trigger: Any modification to settings.json, CLAUDE.md, or hook scripts
#
# This hook prevents "Privilege Self-Escalation" -- a scenario where the AI agent
# modifies its own permission settings to remove deny rules or disable security.
# All config change attempts are logged to an audit trail for forensic analysis.
# =============================================================================

# Read the JSON payload from stdin
INPUT=$(cat)

# Extract metadata about the config change
SOURCE=$(echo "$INPUT" | jq -r '.source // "unknown"' 2>/dev/null)
FILE_PATH=$(echo "$INPUT" | jq -r '.file_path // "unknown"' 2>/dev/null)
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Define audit log location
AUDIT_LOG=".claude/audit-config.log"

# Log the attempted change
echo "[${TIMESTAMP}] CONFIG CHANGE ATTEMPTED | source: ${SOURCE} | file: ${FILE_PATH}" >> "${AUDIT_LOG}" 2>/dev/null

# =============================================================================
# BLOCK ALL CONFIG CHANGES
# 
# Rationale: Security configuration should NEVER be modified by the AI agent.
# Any legitimate configuration change must be made directly by the user.
# This prevents:
#   - Removing deny rules from permissions
#   - Disabling sandbox settings
#   - Modifying hook scripts to bypass security
#   - Changing defaultMode to bypassPermissions
# =============================================================================

# Output block decision to stderr for Claude Code to process
cat <<'EOF' >&2
{
  "hookSpecificOutput": {
    "hookEventName": "ConfigChange",
    "decision": "block",
    "reason": "SECURITY: Configuration changes by AI agent are prohibited. All security settings (settings.json, CLAUDE.md, hooks) must be modified exclusively by the user. This policy prevents privilege self-escalation attacks.",
    "additionalContext": "If configuration changes are needed, ask the user to make them manually."
  }
}
EOF

# Log the block action
echo "[${TIMESTAMP}] CONFIG CHANGE BLOCKED | source: ${SOURCE} | file: ${FILE_PATH}" >> "${AUDIT_LOG}" 2>/dev/null

exit 2
