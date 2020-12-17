from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='aio-py-rq',
    version='1.0.2',
    packages=['aiopyrq'],
    url='https://github.com/heureka/aio-py-rq',
    license='MIT',
    author='Heureka.cz',
    author_email='podpora@heureka.cz',
    description='Redis queue for Asynchronous Python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "aioredis>1.3.1"
    ]
)