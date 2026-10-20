FROM python:3.11-slim
WORKDIR /app
COPY distributor_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/pharmacy.proto /proto/pharmacy.proto
RUN mkdir -p /app/app/generated && \
    python -m grpc_tools.protoc \
        -I/proto \
        --python_out=/app/app/generated \
        --grpc_python_out=/app/app/generated \
        /proto/pharmacy.proto && \
    sed -i 's/import pharmacy_pb2/from . import pharmacy_pb2/' \
        /app/app/generated/pharmacy_pb2_grpc.py && \
    touch /app/app/generated/__init__.py
COPY distributor_service/app/ ./app/
RUN touch /app/app/__init__.py
RUN mkdir -p /data
EXPOSE 50051
CMD ["python", "-m", "app.server"]
