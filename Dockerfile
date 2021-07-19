FROM python:3.8

WORKDIR /pv_live

COPY requirements.txt /pv_live/requirements.txt

RUN apt-get -qq update && apt-get -qq install -y \
    curl \
    git \
    wget \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    libgdal-dev \
    python-gdal \
    gdal-bin \
    > /dev/null

RUN pip install --no-cache-dir git+https://github.com/SheffieldSolar/PV_Live-API.git@0.8 > /dev/null

#RUN pip install --no-cache-dir -r /pv_live/requirements.txt > /dev/null
#COPY . /pv_live/

CMD ["python", "/pv_live/pv_tracker.py", "-h"]
