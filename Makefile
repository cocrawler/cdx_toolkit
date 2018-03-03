.PHONY: pytest test distclean dist install

pytest:
	PYTHONPATH=. py.test

distclean:
	rm dist/*

dist: distclean
	python ./setup.py --long-description | rst2html --exit-status=2 2>&1 > /dev/null
	python ./setup.py bdist_wheel
	twine upload dist/* -r pypi

install:
	python ./setup.py install

