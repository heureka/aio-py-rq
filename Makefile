.PHONY: build test

build:
	docker build . -t aiopyrq:latest

test: build
	docker-compose up tests tests7 tests6 tests5
