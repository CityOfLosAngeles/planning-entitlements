planning-entitlements
==============================

Land use permitting analysis for City Planning

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── data                     <- A directory for local, raw, source data.
    ├── gis                      <- A directory for local geospatial data.
    ├── models                   <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks                <- Jupyter notebooks.
    |
    ├── outputs                  <- A directory for outputs such as tables created.
    |
    ├── processed                <- A directory for processed, final data that is used for analysis.
    │
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports                  <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures              <- Generated graphics and figures to be used in reporting
    │
    │
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment, e.g.
    │                               generated with `pip freeze > requirements.txt`
    │
    ├── setup.py                 <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                      <- Source code for use in this project.
    |
    ├── visualization            <- A directory for visualizations created.
    


--------

### Setting up a Conda Environment 

1. `conda create --name my_project_name` 

2. `source activate my_project_name
3. `conda install --file conda-requirements.txt -c conda-forge` 
4. `pip install requirements.txt`

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
