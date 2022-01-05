.PHONY: init test clean_coverage test_coverage distclean distcheck dist install

init36:
	# packages are deprecating support, so this uses exact versions
	pip install -r requirements.txt

init:
	pip install --use-feature=in-tree-build .

unit:
	PYTHONPATH=. py.test --doctest-modules cdx_toolkit tests/unit -v -v

test:
	PYTHONPATH=. py.test --doctest-modules cdx_toolkit tests -v -v
	PYTHONPATH=. examples/iter-and-warc.py

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	PYTHONPATH=. coverage run -a --source=cdx_toolkit,examples examples/iter-and-warc.py
	PYTHONPATH=. py.test --doctest-modules --cov-report=xml --cov-append --cov cdx_toolkit tests -v -v
	coverage report

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	echo "reminder, you must have tagged this commit or you'll end up failing"
	echo "  git tag v0.x.x"
	echo "  git push --tags"
	python ./setup.py sdist
	twine upload dist/* -r pypi

install:
	python ./setup.py install

