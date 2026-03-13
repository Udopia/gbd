rm -rf dist/
# sudo python3 -m pip install --upgrade build twine
python3 -m build
twine upload dist/*
