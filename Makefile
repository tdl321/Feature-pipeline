.PHONY: start stop pipeline dashboard

start:
	docker compose up -d

stop:
	docker compose down

pipeline:
	python scripts/run_pipeline.py $(MARKETS)

dashboard:
	open http://localhost:3000
