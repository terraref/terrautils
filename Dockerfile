FROM ubuntu:16.04
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# TODO: Use non-dev versions
RUN apt-get -q -y update \
    && apt-get install -y \
        software-properties-common \
        build-essential \
        python-pip \
    && add-apt-repository ppa:ubuntugis/ppa \
    && apt-get -q -y update \
    && apt-get install -y \
        libpng12 \
        libfreetype6 \
        libjpeg-dev \
        libgdal20 \
        gdal-bin \
        python-gdal \
        python-pkgconfig \
    && rm -rf /var/lib/apt/lists/*

# TODO: Create intermediary NCO Container for subset of extractors

COPY requirements.txt /src/requirements.txt
RUN pip install -r /src/requirements.txt \
    && pip install pytest twine

WORKDIR /src
COPY . /src
RUN pip install --editable .

CMD pytest
