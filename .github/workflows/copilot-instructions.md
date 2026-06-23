# GitHub Copilot Instructions

This document defines the guardrails and expectations for GitHub Copilot when handling issues and creating PRs in this repository.

---

## Vulnerability Auto Resolution via Copilot Agent

The following rules apply when Copilot is assigned a security vulnerability issue created by the automated scan workflow.

### Scope

Copilot is authorized to work on issues labeled with:
- `copilot` — general tasks
- `security` — security vulnerability fixes

Copilot **must not**:
- Change database deployment scripts (`mysqldb/liquibase/`, `mysqldb/stored_procedures/`, `mysqldb/triggers/`)
- Alter environment configuration files (`blobstorage/config/dev/`, `blobstorage/config/stage/`)
- Modify ADF pipeline definitions (`adf/pipeline/`, `adf/linkedService/`)
- Introduce new dependencies not already present in `pyproject.toml` or `requirements.txt`
- Make changes outside the scope described in the issue

Copilot **may** modify CI/CD workflow files (`.github/workflows/`) **only** when the issue explicitly requires it. Any PR that touches workflow files must include a `⚠️ Workflow change` note at the top of the PR description to flag it for careful human review.

---

## Security Fix Guidelines

When fixing a security vulnerability issue, Copilot must follow these rules:

### Dependency Vulnerabilities
1. **Only bump the version** of the affected package — do not swap to a different package
2. **Use the minimum safe version** that resolves the CVE — do not jump to latest unless the issue explicitly says so
3. **Update only `pyproject.toml` and `poetry.lock`** — do not modify any `.py` source files unless the issue explicitly requires it
4. **Do not change other dependencies** that were not flagged in the issue

### Hardcoded Secrets
1. Replace hardcoded credentials with `dbutils.secrets.get(scope='<scope>', key='<key>')`
2. Add a comment explaining the secret scope and key name expected
3. Do not introduce any new hardcoded values

---

## PR Requirements

Every PR created by Copilot must:
- Include `Fixes #<issue_number>` in the **PR body** (not just the title) so GitHub automatically closes the linked issue on merge
- Include a brief description of what was changed and why
- Be targeted at the `main` branch
- Pass all existing CI checks before requesting review
- Request review from the issue author

---

## Commit Message Format

```
fix(<scope>): <short description>

Fixes #<issue_number>
<optional longer description>
```

**Example:**
```
fix(deps): bump requests from 2.6.0 to 2.31.0

Fixes #78
Resolves CVE-2018-18074 - credential exposure via HTTP redirect
```

---

## Out of Scope

If an issue asks Copilot to do any of the following, it must leave a comment explaining it cannot proceed and tag the issue author:
- Refactor or rewrite core framework logic in `abcframework/`
- Modify database schemas or migration scripts
- Change environment-specific configuration (dev/stage configs)
- Alter ADF pipelines or linked services