all: build

build:
	docker compose build

up:
	docker compose up -d

test:
	curl -sSL 'http://localhost:8083/connector-plugins?connectorsOnly=false' \
		| jq '.[] | select(.type=="transformation") | select (.class | startswith("io.aiven"))'
