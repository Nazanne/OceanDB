FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml .
COPY src src
RUN pip install --no-cache-dir .[project]
COPY docker_build/entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
