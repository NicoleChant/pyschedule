# Author: Nikol Chantzi

from setuptools import setup, find_packages

with open("requirements.txt", "r") as f:
    requirements = [file.strip() for file in f.readlines()]

setup(
        version="1.0",
        author="Nikol Chantzi",
        packages=find_packages(),
        install_requires=requirements,
        scripts=['bin/sassign']
)
