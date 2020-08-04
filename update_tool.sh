sudo python3 setup.py develop sdist bdist_wheel
twine upload dist/*
sudo rm -Rf gbd_tools.egg-info dist build
