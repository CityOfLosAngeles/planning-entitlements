FROM cityofla/ita-data-civis-lab:sha-4888c7e  

COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt