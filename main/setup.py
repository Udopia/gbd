from setuptools import setup

setup(name='global_benchmark_database_tool',
      version='2.2.10',
      description='A tool for global benchmark management',
      long_description=open('README.md', 'rt').read(),
      long_description_content_type="text/markdown",
      url='https://github.com/Weitspringer/gbd',
      author='Markus Iser, Luca Springer',
      author_email='',
      license='MIT',
      classifiers=[
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.7",
      ],
      packages=['gbd_tool', 'gbd_tool/database',
                'gbd_tool/hashing'],
      include_package_data=True,
      install_requires=[
          'flask',
          'setuptools',
          'tatsu',
      ],
      zip_safe=False)
