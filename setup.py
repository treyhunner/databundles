#!/usr/bin/env python

#from distribute_setup import use_setuptools
#use_setuptools()

from setuptools import setup, find_packages #@UnresolvedImport


import sys, re

from databundles import __version__

if sys.version_info <= (2, 6):
    error = "ERROR: databundles requires Python Version 2.7 or above...exiting."
    print >> sys.stderr, error
    sys.exit(1)

def readme():
    with open("README") as f:
        return f.read()

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

setup(name = "databundles",
      version = __version__,
      description = "Data packaging and distribution framework",
      long_description = readme(),
      author = "Eric Busboom",
      author_email = "eric@clarinova.com",
      url = "https://github.com/clarinova/databundles",
      install_requires=['distribute'],
      xpackages = ["databundles", 
                  "databundles.client",
                  "databundles.server",
                  "databundles.geo",
                  "databundles.sourcesupport",
                  ],
      packages = find_packages(), 
      scripts=['scripts/dbundle', 'scripts/dbmanage'],
      package_data = {"databundles": ["support/*"]},
      license = "",
      platforms = "Posix; MacOS X; Windows",
      classifiers = [],
      #zip_safe=False,
      #install_requires = parse_requirements('requirements.txt'),
      #dependency_links = parse_dependency_links('requirements.txt'),
      )
