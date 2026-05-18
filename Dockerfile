FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip
RUN pip install -e .

ENV DAGSTER_HOME=/tmp

CMD ["dagster", "api", "grpc", "-m", "orchestration.orchestration"] 