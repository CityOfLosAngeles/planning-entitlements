planning-entitlements
==============================

Land use permitting analysis for Department of City Planning

## Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    |
    ├── README.md                <- The top-level README for developers using this project.
    |
    ├── catalogs                 <- A directory for data sources used in repo.
    │   └── catalog.yml          <- Catalog for data sources in S3 or databases.
    │   └── open-data.yml        <- Catalog for data from open data portals.
    ├── manifest.yml             <- Save a copy of open data into S3 with `make mirror`
    |
    ├── laplan                   <- A python package for planning-related utility functions.
    ├── laplan_README.md         <- README for the `laplan` pacakage.
    |
    ├── data                     <- A directory for local, raw, source data.
    ├── gis                      <- A directory for local geospatial data.
    ├── models                   <- Trained and serialized models, model predictions, or model summaries.
    ├── outputs                  <- A directory for outputs such as tables created.
    ├── processed                <- A directory for processed, final data that is used for analysis.
    |
    ├── src                      <- Source code for use in this project.
    ├── notebooks                <- Jupyter notebooks.
    |
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    ├── reports                  <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures              <- Generated graphics and figures to be used in reporting.
    ├── visualization            <- A directory for visualizations created.
    |
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment, e.g.
    │                               generated with `pip freeze > requirements.txt`
    ├── setup.py                 <- Makes project pip installable (pip install -e .) so src can be imported
    |
    

--------

## Starting with JupyterHub

1. Sign in with credentials. [More details on getting started here.](https://cityoflosangeles.github.io/best-practices/getting-started-github.html) 
2. Launch a new terminal and clone repository: `git clone https://github.com/CityOfLosAngeles/planning-entitlements.git`
3. Change into directory: `cd planning-entitlements`
4. Make a new branch and start on a new task: `git checkout -b new-branch`


## Starting with Docker

1. Start with Steps 1-2 above
2. Build Docker container: `docker-compose.exe build`
3. Start Docker container `docker-compose.exe up`
4. Open Jupyter Lab notebook by typing `localhost:8888/lab/` in the browser.

## Setting up a Conda Environment 

1. `conda create --name my_project_name` 
2. `source activate my_project_name`
3. `conda install --file conda-requirements.txt -c conda-forge` 
4. `pip install -r requirements.txt`

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


## References
### More Docs
* [Data Workflow](./catalogs/data-workflow-notes.md) 
* [Scripts and Notebooks](./scripts_notebooks_README.md)
* [Laplan Utility Functions](./laplan_README.md)

Other reference docs are stored in the `references` subfolder. Useful website links are listed here:

* [Guide to Zoning String](https://planning.lacity.org/zoning/guide-current-zoning-string)
* [Zoning Code (warning: clunky online doc)](https://www.google.com/url?sa=j&url=http%3A%2F%2Flibrary.amlegal.com%2Fnxt%2Fgateway.dll%2FCalifornia%2Flapz%2Fmunicipalcodechapteriplanningandzoningco%3Ff%3Dtemplates%24fn%3Ddefault.htm%243.0%24vid%3Damlegal%3Alapz_ca&uct=1570026728&usg=zjcgvRShEnWEJBb0m-tfFIOaHZo.&source=chat)
* [PCTS Prefix & Suffix Report](https://planning.lacity.org/resources/prefix-suffix-report)
* [Transit Oriented Communities (TOC) Guidelines](https://planning.lacity.org/ordinances/docs/toc/TOCGuidelines.pdf)
