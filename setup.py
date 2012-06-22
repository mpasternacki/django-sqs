#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='django-sqs',
    version="0.2",
    description='django FormFields using the Chosen javascript plugin for jQuery',
    author='Maciej Pasternacki',
    author_email='maciej@pasternacki.net',
    url='https://github.com/Fandekasp/django-sqs',
    packages=find_packages(),
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Framework :: Django',
    ],
    include_package_data=True,
    zip_safe=False,
)

