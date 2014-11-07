#!/usr/bin/env python

from distutils.core import setup

setup(name='sjq',
      version='0.1.1',
      description='SJQ - Simple Job Queue - Single host, resource aware, batch job scheduler',
      author='Marcus Breese',
      author_email='marcus@breese.com',
      url='http://github.com/mbreese/sjq',
      packages=['sjq'],
      scripts=['bin/sjq'],
     )
