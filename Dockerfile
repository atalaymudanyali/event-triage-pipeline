FROM node:22-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ src/
RUN uv sync --no-dev
COPY --from=frontend /app/frontend/dist frontend/dist
EXPOSE 8000
CMD ["uv", "run", "api"]
