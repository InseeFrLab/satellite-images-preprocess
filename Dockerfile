FROM inseefrlab/onyxia-python-pytorch:py3.12.6

ENV PROJ_LIB=/opt/conda/share/proj

COPY requirements.txt requirements.txt

RUN conda install -c conda-forge gdal=3.9.3 -y &&\
    pip install -r requirements.txt
