.PHONY: init pytest test distclean dist install

init:
	pip install -r requirements.txt

pytest:
	python ./setup.py test

test:
	python ./setup.py test

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	PYTHONPATH=. py.test --doctest-modules --cov-report= --cov-append --cov cdx_toolkit tests -v -v
	PYTHONPATH=. examples/iter-and-warc.py --cov-report= --cov-append --cov cdx_toolkit tests -v -v
	coverage report

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	python ./setup.py sdist
	twine upload dist/* -r pypi

install:
	python ./setup.py install

