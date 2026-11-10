FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# Warm the Ivy cache so the Delta jars are baked into the image instead of
# downloaded on every container start.
RUN SPARK_DRIVER_MEMORY=1g python -c "from lakehouse.spark import get_spark; get_spark().stop()"

ENV LAKE_ROOT=/data/lake
ENTRYPOINT ["python", "-m", "lakehouse"]
CMD ["run", "--month", "2024-01"]
