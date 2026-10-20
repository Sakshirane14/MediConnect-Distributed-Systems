FROM python:3.11-slim
WORKDIR /app
COPY storage_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY storage_service/app/ ./app/
RUN mkdir -p /storage
EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
