import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="neo4j-db-utils",
    version="1.1.0",
    author="Jeff Fischer",
    author_email="jeff.fischer@benedat.com",
    description="Utilities for working with Neo4j",
    long_description=long_description,
    long_description_content_type="text/restructuredtext",
    url="https://github.com/jfischer/neo4j-db-utils",
    packages=setuptools.find_packages(),
    scripts=['bin/neoctl'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
