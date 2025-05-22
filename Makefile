docker_reset:
	docker-compose down -v && docker-compose up -d
init_db:
	CHERRY_INIT_DB=true uv run scripts/main.py
run:
	uv run scripts/main.py

