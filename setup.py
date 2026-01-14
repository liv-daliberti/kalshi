from __future__ import annotations

from pathlib import Path

from setuptools import find_packages, setup


def _read_requirements(filename: str) -> list[str]:
    path = Path(__file__).resolve().parent / filename
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


setup(
    name="kalshi-ingestor",
    version="0.0.0",
    description="Kalshi hourly/daily market ingestor",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.9",
    install_requires=_read_requirements("requirements.txt"),
    extras_require={
        "dev": [
            "flake8>=7.0.0",
            "pylint>=3.0.0",
            "ruff>=0.14.0",
        ],
    },
)
