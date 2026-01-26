qa:
	python -m ruff check .
	python -m mypy .
	python -m pytest -q

run:
	streamlit run app/Home.py
