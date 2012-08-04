#!/usr/bin/env python

from setuptools import setup, find_packages
import sys

if sys.version < '2.7':
    sys.exit('ERROR: Sorry, python 2.7 is required for this application.')


# zip_save=False  : database.Database.create uses __file__
setup(name='databundles',
      version='0.0.9',
      description='Databundles Management Library',
      long_description=open('README').read(),
      author='Eric Busboom',
      license='http://www.opensource.org/licenses/ISC',
      author_email='eric@clarinova.com',
      url='',
      keywords='',
      packages = ['databundles','sourcesupport'],
      package_dir = {'databundles':'src/databundles',
                     'sourcesupport':'src/sourcesupport'
                     },
      include_package_data = True,
      package_data = { 'databundles' : ['support/*']},
      zip_safe=False,
      install_requires=[
        'sqlalchemy>=0.7',
		'pyyaml',
        'petl',
        'beautifulsoup4',
      ]
     )


