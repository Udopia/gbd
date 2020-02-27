from setuptools import setup, find_packages

setup(name='global_benchmark_database_tool',
  version='2.6.4',
  description='Maintenance of Benchmark Instances and their Attributes',
  long_description=open('README.md', 'rt').read(),
  long_description_content_type="text/markdown",
  url='https://github.com/Udopia/gbd',
  author='Markus Iser, Luca Springer',
  author_email='gbd@informatik.kit.edu',
  packages=["gbd_tool", "gbd_server"],
  scripts=["gbd.py", "server.py"],
  include_package_data=True,
  install_requires=[
      'flask',
      'flask_limiter',
      'setuptools',
      'tatsu',
  ],
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
