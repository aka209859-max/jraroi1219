---
paths:
  - ".env*"
  - "**/.env*"
  - "**/credentials.*"
  - "**/*.key"
  - "**/*.pem"
  - "**/*.secret"
  - "**/settings.local.json"
  - "src/auth/**"
  - "src/config/**"
  - "config/**"
---

# Security Rules -- Loaded dynamically when accessing sensitive paths

## Why this file exists

This rule file is automatically loaded by Claude Code ONLY when you attempt to access
files matching the paths defined above. This keeps the main CLAUDE.md lean while ensuring
security instructions are enforced exactly when they matter.

## Absolute Rules for Sensitive File Areas

### Environment & Credential Files (.env*, credentials.*, *.key, *.pem)

**DO NOT** read, write, copy, move, output, or log these files under any circumstances.

These files contain production database passwords, API keys, and authentication tokens.
If their contents are loaded into your context window, they will be transmitted to
Anthropic's cloud servers for inference -- causing irreversible credential exposure
that could compromise the user's production infrastructure and financial accounts.

If you need to reference environment variable NAMES (not values) for configuration:
- Ask the user to confirm which variable names exist
- Use placeholder values like `YOUR_API_KEY_HERE` in code
- Never attempt to read the actual file to "check what's there"

### Authentication & Authorization Code (src/auth/**)

When editing authentication logic:
- NEVER remove or weaken existing validation (JWT verification, token expiry checks)
- NEVER disable CORS restrictions or authentication middleware
- ALWAYS preserve existing security headers
- If you need to modify auth flow, explain the change to the user FIRST and get approval

### Configuration Files (src/config/**, config/**)

- Treat all configuration as potentially containing secrets
- Never output configuration values to console.log or print statements
- When writing new config, always use environment variable references, not hardcoded values

### Database Connection & Query Safety

When working near database configuration or query code:
- NEVER execute DROP, TRUNCATE, or DELETE without WHERE clause
- ALWAYS use parameterized queries (never string concatenation for SQL)
- NEVER log or output database connection strings
- Confirm destructive migrations with the user before applying

## Incident Response

If you accidentally access or output any sensitive data:
1. IMMEDIATELY notify the user
2. Do NOT attempt to "undo" by deleting logs -- this makes forensics harder
3. Recommend the user rotate the exposed credentials
4. Log the incident details for the user's review
