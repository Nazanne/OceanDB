FROM python:3.11-slim
WORKDIR /app
COPY docker_build/entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
