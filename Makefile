.PHONY: create_env update_env install

create_env:
	conda env create -f environment.yml

update_env:
	conda env update -f environment.yml --prune

install:
	pip install -e .
