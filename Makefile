.PHONY: pip conda install mirror

conda: conda-requirements.txt
	conda install -c conda-forge --yes --file conda-requirements.txt

pip: requirements.txt
	pip install -r requirements.txt
	pip install -e .

install: conda pip

mirror:
	intake-dcat mirror manifest.yml > catalogs/open-data.yml

clean_census:
	python src/C2_clean_census.py 
	python src/C3_clean_values.py 
	python src/C4_subset_census.py