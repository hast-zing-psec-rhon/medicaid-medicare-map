# Secret Scanning Policy

This repository follows a zero-plaintext-secret policy.

## Required controls

1. **No secrets in Git history**: API keys, access tokens, private keys, passwords, and credentials must never be committed.
2. **Pre-commit enforcement**: Every local commit must pass the `gitleaks` pre-commit hook.
3. **CI enforcement**: Pull requests and protected branch pushes must pass `Secret Scan (Gitleaks)`.
4. **Periodic deep scan**: `Secret Scan (TruffleHog)` runs on a weekly schedule and on manual dispatch.
5. **Remediation SLA**: Any exposed credential must be rotated immediately and removed from history.

## Local developer setup

```bash
brew install gitleaks pre-commit
pre-commit install
pre-commit run --all-files
```

## Incident response minimum

- Revoke/rotate compromised credentials first.
- Remove leaked material from history (e.g., `git filter-repo` / BFG) and force-push with coordination.
- Confirm clean state by re-running gitleaks and trufflehog scans.
