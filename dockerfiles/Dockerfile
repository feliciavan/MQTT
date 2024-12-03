FROM python:3.12-slim

WORKDIR /proj

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

RUN rm requirements.txt