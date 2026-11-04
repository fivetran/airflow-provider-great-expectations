from __future__ import annotations

from typing import Final

import pytest

# Every test must carry one of these markers, so that some CI job selects it.
# pytest.ini registers exactly this set.
TIER_MARKERS: Final[frozenset[str]] = frozenset(
    {
        "unit",
        "integration",
        "postgres",
        "spark_integration",
        "spark_connect_integration",
    }
)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Fail the run if any collected test lacks a tier marker.

    This runs before `-m`, `-k` and `--deselect` filter the items, so it sees
    every collected test whatever the run selects. Markers outside
    `TIER_MARKERS`, such as `parametrize`, don't count.
    """
    untiered = [
        item.nodeid
        for item in items
        if not any(mark.name in TIER_MARKERS for mark in item.iter_markers())
    ]
    if untiered:
        listing = "\n".join(f"  {nodeid}" for nodeid in untiered)
        raise pytest.UsageError(
            f"{len(untiered)} test(s) have no tier marker. "
            f"Mark each with one of: {', '.join(sorted(TIER_MARKERS))}.\n{listing}"
        )
