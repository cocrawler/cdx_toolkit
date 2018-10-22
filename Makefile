.PHONY: init pytest test distclean dist install

init:
	pip install -r requirements.txt

pytest:
	python ./setup.py test

test: pytest
	(cd tests; PYTHONPATH=.. ./test.sh)

clean_coverage:
	rm -f .coverage
	rm -f tests/.coverage

test_coverage: clean_coverage
	PYTHONPATH=. py.test --cov-report= --cov-append --cov cdx_toolkit tests
ifdef MISSING
	coverage report -m > MISSING.pytest
endif
	(cd tests; PYTHONPATH=.. COVERAGE='coverage run -a --source=../cdx_toolkit,../scripts' ./test.sh)
	coverage combine .coverage tests/.coverage
ifdef MISSING
	coverage report -m | tee MISSING.all
endif
	coverage report

distclean:
	rm -rf dist/

dist: distclean
	python ./setup.py --long-description | rst2html --exit-status=2 2>&1 > /dev/null
	python ./setup.py sdist
	twine upload dist/* -r pypi

install:
	python ./setup.py install

