"""Apply SQL roles and create per-service DB users."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg
from psycopg import sql
from dotenv import load_dotenv


def _env_url(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing {name} in environment")
    return value


def _password_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.password:
        raise RuntimeError(f"No password in DATABASE_URL: {url}")
    return parsed.password


def main() -> None:
    env_path = os.getenv("ENV_FILE", str(Path(__file__).resolve().parents[1] / ".env"))
    load_dotenv(dotenv_path=env_path)
    dsn = _env_url("DATABASE_URL")
    roles_sql = Path(__file__).resolve().parents[1] / "sql" / "roles.sql"
    roles_text = roles_sql.read_text(encoding="utf-8")

    users = {
        "kalshi_rest_user": _env_url("DATABASE_URL_REST"),
        "kalshi_ws_user": _env_url("DATABASE_URL_WS"),
        "kalshi_worker_user": _env_url("DATABASE_URL_WORKER"),
        "kalshi_rag_user": _env_url("DATABASE_URL_RAG"),
        "kalshi_portal_user": _env_url("DATABASE_URL_PORTAL"),
    }
    role_map = {
        "kalshi_rest_user": "kalshi_rest",
        "kalshi_ws_user": "kalshi_ws",
        "kalshi_worker_user": "kalshi_worker",
        "kalshi_rag_user": "kalshi_rag",
        "kalshi_portal_user": "kalshi_portal",
    }

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(roles_text)
            for user, url in users.items():
                cur.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s",
                    (user,),
                )
                exists = cur.fetchone() is not None
                password = _password_from_url(url)
                if exists:
                    cur.execute(
                        sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {} INHERIT").format(
                            sql.Identifier(user),
                            sql.Literal(password),
                        )
                    )
                else:
                    cur.execute(
                        sql.SQL("CREATE USER {} WITH LOGIN PASSWORD {} INHERIT").format(
                            sql.Identifier(user),
                            sql.Literal(password),
                        )
                    )
            for user, role in role_map.items():
                cur.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(role),
                        sql.Identifier(user),
                    )
                )
        conn.commit()

    print("Applied roles.sql and created per-service users.")


if __name__ == "__main__":
    main()
