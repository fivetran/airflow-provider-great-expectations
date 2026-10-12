#!/usr/bin/env python3
"""Verify the version of the Package with the version in Git tag."""

import os
import re
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

path_of_constants = Path(
    repo_dir / "great_expectations_provider" / "common" / "constants.py"
)
version_file = path_of_constants.read_text()
version = re.findall('VERSION = "(.*)"', version_file)[0]

git_tag_name = os.getenv("GITHUB_TAG_NAME", "")

if not git_tag_name:
    raise SystemExit("No Git tag found. GITHUB_TAG_NAME must be set.")

if version != git_tag_name:
    raise SystemExit(
        f"The version in {path_of_constants} ({version}) does not match "
        f"the Git Tag ({git_tag_name})."
    )
