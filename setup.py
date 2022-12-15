from setuptools import setup, find_packages

setup(name='gbd_tools',
  version='4.2.3',
  description='GBD Benchmark Database Tools: Maintenance of Benchmark Instances and their Attributes',
  long_description=open('README.md', 'rt').read(),
  long_description_content_type="text/markdown",
  url='https://github.com/Udopia/gbd',
  author='Markus Iser, Karlsruhe Institute of Technology (KIT)',
  author_email='markus.iser@kit.edu',
  packages=[
    "gbd_core", 
    "gbd_core.init",
    "gbd_apps",
    "gbd_server"
  ],
  scripts=[
    "gbd.py", 
    "server.py"
  ],
  include_package_data=True,
  setup_requires=[
    'wheel',
    'setuptools'
  ],
  install_requires=[
    'flask',
    'flask_limiter',
    'tatsu',
    'numpy',
    'mip',
    'pandas',
    'matplotlib',
    'waitress',
    'pebble',
    'scikit-learn',
    'iteration_utilities'
  ],
  install_obsoletes=['global-benchmark-database-tool'],
  classifiers=[
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Programming Language :: Python :: 3"
  ],
  entry_points={
    "console_scripts": [
        "gbd = gbd:main",
        "gbd-server = server:main"
    ]
  }
)
