SHELL := /bin/sh
PY ?= python3

.PHONY: help test demo verify clean

help:
	@echo "make test     运行剖析单测"
	@echo "make demo     生成两批数据并比较基线"
	@echo "make verify   断言 profile、漂移和告警"

test:
	PYTHONPYCACHEPREFIX=/tmp/profilr-pycache $(PY) -m unittest discover -s tests -v

demo:
	$(PY) -m profilr.cli demo

verify:
	sh scripts/verify.sh

clean:
	rm -rf /tmp/profilr /tmp/profilr-pycache
