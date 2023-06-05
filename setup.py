from setuptools import setup, find_namespace_packages
from version import version

with open('README.rst') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='bigquery-operator',
    version=version,
    author='Augustin Barillec',
    author_email='augustin.barillec@gmail.com',
    description='Wrapper for usual operations on a fixed BigQuery dataset.',
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['bigquery_operator*']),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux'],
    project_urls={
        'Documentation':
            'https://bigquery-operator.readthedocs.io/en/latest/',
        'Source': 'https://github.com/augustin-barillec/bigquery-operator'}
)
