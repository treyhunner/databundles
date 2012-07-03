#!/usr/bin/env python

from setuptools import setup, find_packages

# zip_save=False  : database.Database.create uses __file__
setup(name='databundles',
      version='0.0.6',
      description='Databundles Management Library',
      long_description=open('README').read(),
      author='Eric Busboom',
      license='http://www.opensource.org/licenses/ISC',
      author_email='eric@clarinova.com',
      url='',
      keywords='',
      packages = find_packages('src'), 
      package_dir = {'':'src'},
      package_data = { 'databundles' : ['support/*']},
      zip_safe=False,
      install_requires=[
        'sqlalchemy>=0.7',
<<<<<<< HEAD
        'pyyaml'
=======
		'pyyaml'
>>>>>>> 25693f0f90021208a35af8dce8da3d6175699f23
      ]
     )
