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
	PYTHONPATH=. py.test --doctest-module --cov-report= --cov-append --cov cdx_toolkit tests
	coverage report

distclean:
	rm -rf dist/

dist: distclean
	python ./setup.py --long-description | rst2html --exit-status=2 2>&1 > /dev/null
	python ./setup.py sdist
	twine upload dist/* -r pypi

install:
	python ./setup.py install

