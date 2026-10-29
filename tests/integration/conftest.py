from __future__ import annotations

import os
import random
import string
from typing import TYPE_CHECKING, Callable, Generator

import pytest
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.config import Config
    from _pytest.nodes import Item


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    missing_markers: list[str] = []
    for item in items:
        if not any(item.iter_markers()):
            missing_markers.append(item.nodeid)
    if missing_markers:
        raise Exception(
            f"Missing markers on the following tests: {', '.join(missing_markers)}"
        )


def rand_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


@pytest.fixture
def table_name() -> str:
    return "test_table"


@pytest.fixture
def postgres_connection_string() -> str:
    pg_user = os.environ["POSTGRES_USER"]
    pg_pw = os.environ["POSTGRES_PASSWORD"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    return f"postgresql+psycopg2://{pg_user}:{pg_pw}@localhost:{pg_port}/{pg_db}"


@pytest.fixture
def load_postgres_data(
    postgres_connection_string: str,
    table_name: str,
) -> Generator[Callable[[list[dict]], None], None, None]:
    """Loads data into a table called `test_table` in the Postgres database.

    This will have a string column called name, and an int column called age.
    This should be enough to cover our use cases.
    """

    def _load_postgres_data(data: list[dict]) -> None:
        engine = create_engine(url=postgres_connection_string)
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(f"CREATE TABLE {table_name} (name VARCHAR(255), age INT);")
            )
            conn.execute(
                text(f"INSERT INTO {table_name} (name, age) VALUES (:name, :age);"),
                data,
            )

    yield _load_postgres_data

    engine = create_engine(url=postgres_connection_string)
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"DROP TABLE {table_name};"))


@pytest.fixture
def load_csv_data() -> Generator[Callable[[Path, list[dict]], None], None, None]:
    def _load_csv_data(path: Path, data: list[dict]) -> None:
        with path.open("w") as f:
            f.write("name,age\n")
            for row in data:
                f.write(f"{row['name']},{row['age']}\n")

    yield _load_csv_data
