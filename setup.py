"""Package generation."""
import os
from setuptools import find_packages, setup


setup(
    name="nodalize",
    version="1.0.0",
    author="Ronan Mouquet",
    author_email="ronan.mouquet@gmail.com",
    description=("todo"),
    license="MIT",
    keywords="",
    url="https://github.com/Ron-Ldn/nodalize",
    packages=find_packages(exclude=['tests*', 'demos*']),
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md")).read(),
    classifiers=[
        "Development Status :: 4 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)
