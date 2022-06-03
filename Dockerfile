FROM python:3.10-slim-bullseye

WORKDIR /aiopyrq

RUN apt update && apt install -y build-essential && pip install poetry

# install requirements
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry config virtualenvs.create false
RUN poetry install --no-root

COPY aiopyrq aiopyrq
COPY tests tests
