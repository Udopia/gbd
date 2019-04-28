import pathlib

from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(name='global-benchmark-database-tool',
      version='0.1',
      description='A tool for global benchmark management',
      long_description=README,
      long_description_content_type="text/markdown",
      url='https://https://github.com/Weitspringer/gbd',
      author='Markus Iser, Luca Springer',
      author_email='',
      license='MIT',
      classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
      ],
      packages=['gbd_tool', 'gbd_tool/database', 'gbd_tool/hashing'],
      include_package_data=True,
      install_requires=[
            'flask',
            'setuptools',
            'tatsu',
      ],
      zip_safe=False)
