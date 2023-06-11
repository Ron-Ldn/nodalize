"""Package generation."""
import os
from setuptools import find_packages, setup


setup(
    name="nodalize",
    version="1.0.0",
    author="Ronan Mouquet",
    author_email="ronan.mouquet@gmail.com",
    description="Framework to organize data transformation flow.",
    long_description_content_type='text/markdown',
    license="MIT",
    keywords="",
    url="https://github.com/Ron-Ldn/nodalize",
    packages=find_packages(exclude=['tests*', 'demos*']),
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md")).read(),
)
