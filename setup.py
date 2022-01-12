"""
setup after sampleproject of pypa
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='isos',
    version='0.0a1',
    description='insearchofsentinel',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    url='https://github.com/MarkusZehner/rcm',

    # license='MIT',
    author='markuszehner',
    author_email='markus.zehner@uni-jena.de',

    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.

        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],

    keywords='sentinel data management',

    python_requires='>=3.6',

    install_requires=['sqlalchemy',
                      'sqlalchemy-utils',
                      'geoalchemy2',
                      'requests==2.22',
                      'shapely',
                      'sentinelsat',
                      'spatialist',
                      'gdal']
)
