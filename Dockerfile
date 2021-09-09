FROM python:3.8

WORKDIR /pv_live

COPY requirements.txt /pv_live/requirements.txt

RUN apt-get -qq update && apt-get -qq install -y \
    curl \
    git \
    wget \
    > /dev/null

RUN pip install --no-cache-dir git+https://github.com/SheffieldSolar/PV_Live-API.git@0.9 > /dev/null

#RUN pip install --no-cache-dir -r /pv_live/requirements.txt > /dev/null
#COPY . /pv_live/

CMD ["python", "/pv_live/pv_tracker.py", "-h"]
