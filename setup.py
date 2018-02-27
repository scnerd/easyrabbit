from setuptools import setup

setup(
    name='easyrabbit',
    version='0.0.3',
    packages=['easyrabbit'],
    url='https://github.com/scnerd/easyrabbit',
    license='MIT',
    author='scnerd',
    author_email='',
    long_description=open('README.rst').read(),
    description='Easy utilities for common RabbitMQ tasks',
    install_requires=[
        'pika>=0.10',
    ]
)
