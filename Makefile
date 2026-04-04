.PHONY: test compile check annotation-loop-help

PYTHON ?= python3

test:
	$(PYTHON) -m unittest discover -s tests -q

compile:
	$(PYTHON) -m py_compile \
		txflow/__init__.py \
		src/txflow/__init__.py \
		src/txflow/cli.py \
		src/txflow/gnn_pipeline.py \
		src/txflow/graph_risk.py \
		src/txflow/ledger_ops.py \
		src/txflow/report_io.py \
		src/txflow/round_ops.py \
		src/txflow/thresholds.py \
		tests/test_modules.py \
		tests/test_package_api.py

check:
	bash scripts/check.sh

annotation-loop-help:
	bash scripts/run_annotation_loop.sh
