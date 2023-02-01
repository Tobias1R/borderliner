from setuptools import setup, find_packages

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

setup(
    name='borderliner',
    version='0.1',
    description='The insightful data pipeline framework',
    author='Tobias Rocha',
    author_email='tobias_rocha@yahoo.com',
    packages=find_packages(),
    install_requires=install_requires,
)