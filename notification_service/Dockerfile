FROM python:3.11-slim
WORKDIR /app
COPY notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY notification_service/app/ ./app/
CMD ["python", "-m", "app.consumer"]
