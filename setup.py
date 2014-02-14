# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic


from setuptools import setup

ld = """Kafka client, basic.
"""

setup(
    name = 'kafka',
    version = '0.0.1',
    author = 'Mik Kocikowski',
    author_email = 'mkocikowski@gmail.com',
    url = 'https://github.com/mkocikowski/estools',
    description = 'Kafka client, basic.',
    long_description = ld,
#     install_requires = [
#         'requests >= 2.1.0',
# #         'pyrax >= 1.6.2',
#     ],
    packages = [
        'kafka',
        'kafka.test',
#         'kafka.client',
#         'kafka.consumer',
#         'kafka.producer',
    ],
    package_data = {
        '': ['README.md'],
    },
#     entry_points = {
#         'console_scripts': [
#             'esload = estools.load.client:main',
#             'esdump = estools.dump.client:main',
#         ]
#     },
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Utilities",
    ],
    license = 'MIT',
#     test_suite = "estools.test.units.suite",
)
