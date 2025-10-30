FROM python:3.9

WORKDIR /app

COPY requirements.txt /app
COPY mqtt_subscriber.py /app
COPY processor.py /app
COPY certs /app/certs

RUN pip install -r requirements.txt

CMD ["python", "processor.py"]