#!/usr/bin/env python

from setuptools import setup, find_packages

# zip_save=False  : database.Database.create uses __file__
setup(name='databundles',
      version='0.0.2',
      description='Databundles Management Library',
      long_description=open('README').read(),
      author='Eric Busboom',
      license='http://www.opensource.org/licenses/ISC',
      author_email='eric@clarinova.com',
      url='',
      keywords='',
      packages = find_packages('src'), 
      package_dir = {'':'src'},
      zip_safe=False 
     )