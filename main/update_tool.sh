#!/bin/bash
sudo python3 setup.py develop; python3 setup.py sdist bdist_wheel;twine upload dist/*;sudo rm -Rf global_benchmark_database_tool.egg-info build dist;
