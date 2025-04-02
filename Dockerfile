FROM python:3.12-bullseye

WORKDIR /app

RUN apt-get update && apt-get upgrade -y

COPY requirements.txt .

RUN pip install -U pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "src/main.py"]
