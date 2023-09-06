FROM python:3.8-slim

WORKDIR /app

RUN pip install elasticsearch

COPY . .

CMD ["tail", "-f", "/dev/null"]
