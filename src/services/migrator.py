"""Schema migrator entrypoint."""

from __future__ import annotations

import logging
import os

import psycopg  # pylint: disable=import-error
from dotenv import load_dotenv  # pylint: disable=import-error

from ..db.db import ensure_schema_compatible, init_schema
from ..core.logging_utils import configure_http_logging, log_format, log_handlers
from ..core.loop_utils import schema_path
from ..core.settings import ENV_FILE

logger = logging.getLogger(__name__)


def _schema_path() -> str:
    """Resolve the schema SQL path relative to this module."""
    return schema_path(__file__)


def main() -> None:
    """Apply the schema migrations to the configured database."""
    load_dotenv(dotenv_path=ENV_FILE)
    logging.basicConfig(
        level=logging.INFO,
        format=log_format("migrator"),
        handlers=log_handlers("migrator"),
    )
    configure_http_logging(logging.INFO)
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is required for migrator.")
    with psycopg.connect(db_url) as conn:
        init_schema(conn, schema_path=_schema_path())
        version = ensure_schema_compatible(conn)
    logger.info("Schema migration complete.")
    if version >= 0:
        logger.info("Schema version=%s", version)


if __name__ == "__main__":
    main()
