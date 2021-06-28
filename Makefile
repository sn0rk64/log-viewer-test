run:
	docker-compose build && docker-compose up
up:
	docker-compose up
restart:
	down up
down:
	docker-compose down -v
