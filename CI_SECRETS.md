# CI Secrets Configuration

This document describes how CI environments gate access to repository secrets.

## GitHub Environments

The test workflows use no secrets and no environments, so a pull request, including one from a fork, runs them with a read-only token and nothing to expose. Two environments remain, each serving one workflow:

- **`pypi`**: serves publishing. The publish job in `ci.yaml` runs in it when a release is published, and it publishes by PyPI trusted publishing, so it holds no stored token.
- **`notifications`**: serves Slack. The jobs in `slack-notifications.yml` run in it and read the Slack bot token (`SLACK_BOT_TOKEN`) from it.

The [CI guide](.github/CI.md) has the details, including the repository settings to check.
