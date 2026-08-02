FROM python:3.11-slim

# git for the docs mirror pull loop; openssh-client for the forced-command
# host diag; iputils-ping for the router-alive / ping-nodes checks
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client iputils-ping \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV TODOS_DB=/data/todos.db \
    DOCS_REPO_DIR=/data/docs-mirror \
    DOCS_INDEX_DB=/data/docs-index.db \
    PORT=8650

VOLUME /data

EXPOSE 8650

CMD ["python", "server.py"]
