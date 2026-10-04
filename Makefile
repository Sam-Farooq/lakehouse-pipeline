.PHONY: up down produce bronze silver gold test lint dbt

up:      ; docker compose up -d
down:    ; docker compose down -v
produce: ; python -m lakehouse.producer.events --rate 500
bronze:  ; python -m lakehouse.streaming.bronze
silver:  ; python -m lakehouse.streaming.silver
gold:    ; python -m lakehouse.batch.gold --date $(DATE)
test:    ; pytest -q
lint:    ; ruff check src tests dags
dbt:     ; cd dbt && dbt run && dbt test
