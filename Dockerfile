FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./models /app/models

CMD ["python", "run_process_coletas.py"]