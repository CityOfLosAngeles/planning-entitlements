FROM cityofla/ita-data-civis-lab:sha-64ec87b  

COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt