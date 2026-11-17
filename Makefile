# Scalable Text Analytics Platform on Hadoop MapReduce
#
# One command does the whole loop:
#   make            -> build + tests + coverage, generate data, run, verify
#   make bench      -> measurement harness + plots
#
# Everything Java runs inside the JDK-11 build image (Docker), because Hadoop 3.3.x
# does not support the JDK 17+ that hosts ship and a real Maven is required.
SHELL := bash
SHELLFLAGS := -eu -o pipefail -c

DATASET ?= 10m
REDUCERS ?= 2
FS ?= local

.PHONY: all build test data run verify bench plots clean clean-all check help \
        cluster-up cluster-down cluster-run fault-tolerance

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

all: build data run verify ## build + test, generate data, run pipeline, verify against reference

build: ## compile, unit-test, integration-test, enforce the 80% coverage gate
	bash scripts/01_build.sh

test: build ## alias: the tests run inside `build`

data: ## generate the corpora (10m, 100m, tiny10k; big sizes with TA_BIG=1)
	bash scripts/02_fetch_data.sh
	bash scripts/03_check_stopwords.sh

run: ## run the whole pipeline for DATASET (FS=local|yarn)
	bash scripts/05_run_all.sh $(DATASET)

verify: ## diff pipeline output against the Python reference (diff must be 0)
	bash scripts/06_verify_reference.sh $(DATASET) out/$(DATASET)-$(FS)

bench: ## run every measurement experiment and draw the plots
	bash scripts/07_benchmark.sh

plots: ## re-draw plots from existing CSVs
	python python/bench_plot.py --summary results/csv/bench-summary.csv \
	  --baseline results/csv/baseline-python.csv \
	  --metrics results/csv/bench-metrics.csv --out results/plots

check: ## determinism + idempotency smoke test (two runs, identical output)
	bash scripts/06_verify_reference.sh $(DATASET) out/$(DATASET)-$(FS)
	bash scripts/05_run_all.sh $(DATASET)
	bash scripts/06_verify_reference.sh $(DATASET) out/$(DATASET)-$(FS)

cluster-up: ## start the 1 NameNode + 3 DataNode Docker cluster
	docker compose -f docker/docker-compose.yml up -d --build

cluster-down: ## stop the cluster (keeps volumes)
	docker compose -f docker/docker-compose.yml down

cluster-run: ## run the pipeline on the cluster (FS=yarn)
	FS=yarn bash scripts/05_run_all.sh $(DATASET)

fault-tolerance: ## kill a DataNode mid-job and prove identical output
	bash scripts/08_fault_tolerance.sh $(DATASET)

clean: ## remove run output and build artefacts
	bash scripts/99_clean.sh outputs
	bash scripts/99_clean.sh build

clean-all: ## also remove generated corpora and benchmark results
	bash scripts/99_clean.sh all
