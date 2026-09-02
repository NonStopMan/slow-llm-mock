# Mock slow-provider LLM service for cbllmgateway QA performance testing.
# QA-only image. Do not deploy to production networks.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

# CONTROL_TOKEN must be provided at deploy time (env/secret), e.g.:
#   docker run -e CONTROL_TOKEN=<generated-secret> -p 8080:8080 mock-slow-llm
# Never bake the token into the image.
CMD ["python", "app.py"]
