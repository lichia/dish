from setuptools import setup, find_packages

setup(
    name="dish",
    version="0.0.0",
    author="James J. Porter",
    author_email="porterjamesj@gmail.com",
    description="distributed shell",
    license="MIT",
    url="https://github.com/porterjamesj/dish",
    packages=find_packages(),
    install_requires=[
        'ipython-cluster-helper == 0.2.19',
        'ipython >= 2.0.0',
        'logbook',
        'pyzmq',
        'cloud'
    ]
)
