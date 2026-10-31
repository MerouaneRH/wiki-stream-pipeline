COMPOSE := docker compose -f infra/docker-compose.yml

.PHONY: install up down clean ps logs produce produce-file test test-integration lint fmt

install:  ## install Python deps
	uv sync

up:  ## start Redpanda + Console, create topics
	$(COMPOSE) up -d --wait redpanda console
	$(COMPOSE) run --rm topics

down:  ## stop the stack, keep data
	$(COMPOSE) down

clean:  ## stop the stack and delete its data
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

produce:  ## stream live Wikipedia edits into Redpanda (Ctrl+C to stop)
	uv run python -m producer.main --replay-dir data/replay

produce-file:  ## replay an NDJSON file: make produce-file FILE=path RATE=50
	uv run python -m producer.main --from-file $(FILE) --rate $(or $(RATE),0)

test:  ## unit tests
	uv run pytest -q

test-integration:  ## integration tests, needs `make up`
	uv run pytest -q -m integration

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

.PHONY: spark spark-once lakehouse-reset

spark:  ## run bronze + silver + gold streaming jobs (10 s micro-batches, Ctrl+C to stop)
	uv run python -m spark.run bronze silver gold

spark-once:  ## process everything available, then stop
	TRIGGER=available_now uv run python -m spark.run bronze silver gold

lakehouse-reset:  ## delete local Delta tables and checkpoints
	rm -rf lakehouse checkpoints

.PHONY: dashboard

dashboard:  ## open the live dashboard at http://127.0.0.1:8501
	PYTHONPATH=. uv run streamlit run dashboard/streamlit_app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true

.PHONY: db-validate db-deploy db-upload db-run db-destroy

DB_VOLUME := dbfs:/Volumes/workspace/wiki_stream/raw/replay

db-validate:  ## check the Databricks bundle
	databricks bundle validate

db-deploy:  ## build the wheel, create schema/volumes/job on Databricks
	databricks bundle deploy

db-upload:  ## copy recorded replay files (complete .ndjson only) to the raw volume
	databricks fs mkdir $(DB_VOLUME)
	for f in data/replay/*.ndjson; do databricks fs cp --overwrite $$f $(DB_VOLUME)/; done

db-run:  ## run the replay job (bronze -> silver -> gold) and wait for it
	databricks bundle run replay

db-destroy:  ## delete the bundle's job, volumes and schema (asks for confirmation)
	databricks bundle destroy

.PHONY: bench

bench:  ## benchmark (needs `make up` and data/replay): 50/s and 500/s, 3 min each
	uv run python -m bench.run_bench --rates 50 500 --duration 180
