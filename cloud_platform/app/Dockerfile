FROM python:3.9

WORKDIR /app

COPY requirements.txt /app
COPY utils /app/utils
COPY processor.py /app
COPY certs /app/certs
COPY trainnning_dataset /app/trainnning_dataset

RUN pip install -r requirements.txt

CMD ["python", "processor.py"]