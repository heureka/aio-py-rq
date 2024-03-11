from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='aio-py-rq',
    version='v4.0.1',
    packages=['aiopyrq'],
    url='https://github.com/heureka/aio-py-rq',
    license='Apache 2.0',
    author='Heureka.cz',
    author_email='podpora@heureka.cz',
    description='Redis queue for Asynchronous Python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "redis>=5.0.0"
    ]
)