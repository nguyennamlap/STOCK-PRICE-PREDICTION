FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir fastapi uvicorn kafka-python requests

EXPOSE 8088
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8088"]

