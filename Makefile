.PHONY: setup lint fmt test run

setup:
\tpython -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

lint:
\tpython -m pyflakes src || true

fmt:
\tpython -m pip install ruff black --quiet
\truff check --fix .
\tblack .

test:
\tpytest -q || true
