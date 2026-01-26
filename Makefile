qa:
	python -m ruff check .
	python -m mypy .
	python -m pytest -q

run:
	PYTHONPATH=. streamlit run app/Home.py
