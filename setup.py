from setuptools import setup

setup(
    name="dish",
    version="0.0.0",
    author="James J. Porter",
    author_email="porterjamesj@gmail.com",
    description="distributed shell",
    license="MIT",
    url="https://github.com/porterjamesj/dish",
    packages=["dish"],
    install_requires=[
        'ipython-cluster-helper',
        'ipython >= 2.0.0',
        'logbook',
        'pyzmq'
    ]
)
