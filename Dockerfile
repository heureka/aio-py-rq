FROM python:3.8-alpine3.13

WORKDIR /aiopyrq

RUN apk add --update --no-cache python3 cargo openssl-dev gcc libffi-dev libressl-dev libc-dev && pip install poetry

# install requirements
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry config virtualenvs.create false
RUN poetry install --no-root

COPY aiopyrq aiopyrq
COPY tests tests
