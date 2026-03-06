# Prefer .venv if present (run: pip install -e . in the venv first)
PY := $(or $(wildcard .venv/bin/python),python)

qa:
	$(PY) -m ruff check .
	$(PY) -m mypy .
	$(PY) -m pytest -q

run:
	PYTHONPATH=. $(PY) -m streamlit run app/Home.py
