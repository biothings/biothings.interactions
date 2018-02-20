"""
setup.py -- package installation for biointeract
"""
from setuptools import setup


setup(name='biointeract',
      version='0.1.4',
      description='A biothings api service for interaction data.',
      url='https://github.com/biothings/biothings.interactions',
      author='Greg Taylor',
      author_email='greg.k.taylor@gmail.com',
      license='Apache License Version 2.0',
      packages=['biointeract'],
      install_requires=[
      ],
      zip_safe=False)

