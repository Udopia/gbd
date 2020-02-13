from setuptools import setup, find_packages

setup(name='global_benchmark_database_tool',
  version='2.4.7',
  description='Maintenance of Benchmark Instances and their Attributes',
  long_description=open('README.md', 'rt').read(),
  long_description_content_type="text/markdown",
  url='https://github.com/Udopia/gbd',
  author='Markus Iser, Luca Springer',
  author_email='gbd@informatik.kit.edu',
  packages=find_packages(),
  scripts=["gbd.py"],
  include_package_data=True,
  install_requires=[
      'flask',
      'setuptools',
      'tatsu',
  ],
  classifiers=[
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Programming Language :: Python :: 3"
  ],
  entry_points={
    "console_scripts": [
        "gbd = gbd:main"
    ]
  }
)
