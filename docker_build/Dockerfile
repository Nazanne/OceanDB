FROM python:3.11-slim
WORKDIR /app
COPY build/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY docker_build/entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
