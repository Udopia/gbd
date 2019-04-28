from setuptools import setup

setup(name='gbd-tool',
      version='0.1',
      description='A tool for global benchmark management',
      url='https://https://github.com/Weitspringer/gbd',
      author='Markus Iser, Luca Springer',
      author_email='',
      license='MIT',
      classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
      ],
      packages=['gbd', 'gbd/database', 'gbd/hashing'],
      include_package_data=True,
      install_requires=[
            'flask',
            'setuptools',
            'tatsu',
      ],
      zip_safe=False)
