sudo rm -rf dist/
# sudo python3 setup.py develop sdist bdist_wheel
# twine upload dist/*
sudo python3 -m pip install --upgrade build twine
sudo python3 -m build
twine upload dist/*
sudo rm -Rf gbd_tools.egg-info dist build
