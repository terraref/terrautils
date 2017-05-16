FROM clowder/pyclowder:2
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -q -q update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        make \
        libpng-dev \
        libjpeg8-dev \
        libfreetype6-dev \
        libnetcdf-dev \
        libblas-dev \
        liblapack-dev \
        libatlas-base-dev \
        python-dev \
        python-tk \
        python-gdal \
    && rm -rf /var/lib/apt/lists/*

COPY terrautils /tmp/terrautils/terrautils
COPY setup.py requirements.txt /tmp/terrautils/

RUN pip install --upgrade  -r /tmp/terrautils/requirements.txt \
    && pip install --upgrade /tmp/terrautils \
    && rm -rf /tmp/terrautils
