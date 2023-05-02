FROM python:3.10

WORKDIR /pv_live

RUN pip install --no-cache-dir pvlive-api > /dev/null

CMD ["pv_live", "-h"]
