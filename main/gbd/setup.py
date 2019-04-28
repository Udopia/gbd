from setuptools import setup

setup(name='gbd-tool',
      version='0.1',
      description='A tool for global benchmark management',
      url='https://https://github.com/Weitspringer/gbd',
      author='Markus Iser, Luca Springer',
      author_email='',
      license='MIT',
      packages=['database', 'hashing'],
      install_requires=[
          'flask',
          'tatsu',
      ],
      zip_safe=False)
