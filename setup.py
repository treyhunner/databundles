#!/usr/bin/env python

from setuptools import setup, find_packages
import sys, re

if sys.version < '2.7':
    sys.exit('ERROR: Sorry, python 2.7 is required for this application.')

def parse_requirements(file_name):
    requirements = []
    for line in open(file_name, 'r').read().split('\n'):
        if re.match(r'(\s*#)|(\s*$)', line):
            continue
        if re.match(r'\s*-e\s+', line):
            requirements.append(re.sub(r'\s*-e\s+.*#egg=(.*)$', r'\1', line))
        elif re.match(r'\s*-f\s+', line):
            pass
        else:
            requirements.append(line)

    return requirements

def parse_dependency_links(file_name):
    dependency_links = []
    for line in open(file_name, 'r').read().split('\n'):
        if re.match(r'\s*-[ef]\s+', line):
            dependency_links.append(re.sub(r'\s*-[ef]\s+', '', line))

    return dependency_links


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
      packages=['databundles', 'databundles.client', 'databundles.server', 'databundles.sourcesupport'],
      package_dir={'databundles': 'src/databundles'},
      package_data={'databundles': ['support/*.*']},
      zip_safe=False,
      install_requires = parse_requirements('requirements.txt'),
      dependency_links = parse_dependency_links('requirements.txt')
     )


