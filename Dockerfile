FROM irose/citywide-civis-lab:497b06e9b88f  

COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt

COPY requirements.txt /tmp/
COPY laplan ./laplan
COPY setup.py .

RUN pip install -r /tmp/requirements.txt