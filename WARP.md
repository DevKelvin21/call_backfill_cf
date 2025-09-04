# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

Repository overview
- Current contents: README.md, LICENSE, and a Python-focused .gitignore. No source code or tooling/config files (no pyproject.toml, requirements.txt, Makefile, tox.ini, noxfile.py, or CI configs) are present yet.
- Implication: Commands for build, lint, and tests are not defined in-repo at this time. Treat this as a nascent Python project until tooling appears.

How to discover commands once tooling is added
- Look for these files and infer commands accordingly:
  - pyproject.toml (preferred): poetry/pdm/uv-backed projects, plus tool configs (ruff, black, mypy, pytest).
  - requirements*.txt / constraints*.txt: plain pip projects.
  - Makefile / Taskfile.yml / justfile: task runners defining canonical workflows.
  - tox.ini / .tox: test matrix via tox.
  - noxfile.py: test sessions via nox.
  - .github/workflows/*.yml: CI pipelines that reveal build/test/lint commands.
- Helpful discovery commands:
  - List config files: ls -la
  - Search for tool sections: rg -n "\[(tool|project)" || rg -n "\bpytest\b|\bruff\b|\bblack\b|\bmypy\b|\bflake8\b|\bnox\b|\btox\b|\bpoetry\b|\bpdm\b|\buv\b" || grep -RIn "pytest|ruff|black|mypy|flake8|nox|tox|poetry|pdm|uv" .

Common commands (conditional on tooling appearing)
- Environment setup (when using venv + pip):
  - python3 -m venv .venv && source .venv/bin/activate
  - pip install -U pip
  - If requirements*.txt exists: pip install -r requirements.txt
  - If pyproject.toml with poetry: poetry install
  - If pyproject.toml with pdm: pdm install
  - If pyproject.toml with uv: uv sync
- Lint/format (only if configured):
  - Ruff: ruff check . and ruff format .
  - Black: black .
  - Mypy: mypy .
- Tests (only if configured):
  - Pytest (all): pytest -q
  - Single test file: pytest -q path/to/test_file.py
  - Single test by node id: pytest -q path/to/test_file.py::TestClass::test_case
  - Coverage (if coverage is present): pytest -q --cov=. --cov-report=term-missing
- Task runners (if present):
  - Makefile targets: make help (if defined) or grep -n "^[_A-Za-z-]*:.*" Makefile
  - nox: nox -l (list) and nox -s tests
  - tox: tox -av (list) and tox -e py

High-level architecture and structure
- There are currently no source directories (e.g., src/, package/, or module files) to summarize. When code is added, prefer a src/ layout for Python packages (e.g., src/<package_name>/) with tests/ for test modules to minimize import path ambiguity.
- Once entrypoints exist (CLI, jobs, or services), identify them by scanning for:
  - __main__.py or console_scripts in pyproject.toml
  - Scripts in a Makefile or CI workflows
  - Orchestrating modules (e.g., main.py, app.py) and their dependency graph

Docs and rules
- README.md: Contains only the repository name at present.
- No CLAUDE.md, Cursor rules (.cursor/rules or .cursorrules), or Copilot instructions (.github/copilot-instructions.md) are present.

Notes for future updates to this file
- When tooling or code is added, update this WARP.md to:
  - Record the exact build/lint/test commands used locally and in CI.
  - Summarize the “big picture” flow (entrypoints, main modules, and how they interact).
  - Reference any important rules files (Claude/Cursor/Copilot) and key README sections.

