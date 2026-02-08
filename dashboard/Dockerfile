FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    flask==3.0.0 \
    psycopg2-binary==2.9.9 \
    anthropic==0.39.0

# Copy application
COPY . /app

# Expose port
EXPOSE 5000

# Run app
CMD ["python", "app.py"]
