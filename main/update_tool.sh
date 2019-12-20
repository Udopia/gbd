sudo python3 setup.py develop
sudo python3 setup.py sdist 
sudo python3 setup.py  bdist_wheel
twine upload dist/*
sudo rm -Rf global_benchmark_database_tool.egg-info dist build
