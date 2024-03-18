FROM inseefrlab/onyxia-python-pytorch:py3.11.6

ENV PROJ_LIB=/opt/mamba/share/proj

COPY requirements.txt requirements.txt

RUN mamba install -c conda-forge gdal=3.8.4 -y &&\
    pip install -r requirements.txt
