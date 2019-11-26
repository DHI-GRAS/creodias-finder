from setuptools import setup, find_packages

setup(
    name='creodias_finder',
    version='v0.1.0',
    description='Query and download data from CreoDias API',
    author='Pantelis Kouris',
    author_email='pako@dhigroup.com',
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'requests',
        'six',
        'tqdm',
        'python-dateutil',
        'shapely',
        'boto3'
    ]
)
