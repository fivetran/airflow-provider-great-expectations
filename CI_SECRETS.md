# CI Secrets Configuration

This document describes how CI environments gate access to repository secrets.

## GitHub Environments

The CI workflow uses two GitHub environments to control access to secrets:

- **internal**: Used for PRs from branches within the repository (has access to secrets)
- **external**: Used for PRs from forks (requires manual approval before secrets are accessible)

This protects secrets from being exposed in PRs from untrusted forks.
