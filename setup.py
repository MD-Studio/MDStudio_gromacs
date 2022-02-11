#! /usr/bin/env python
# -*- coding: utf-8 -*-

# package: mdstudio_gromacs
# file: setup.py
#
# Part of ‘mdstudio_gromacs’, a package providing GROMACS Molecular Dynamics
# functionality for the MDStudio package.
#
# Copyright © 2018 Marc van Dijk, VU University Amsterdam, the Netherlands
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages

distribution_name = 'mdstudio_gromacs'

setup(
    name=distribution_name,
    version=1.0,
    description='MDStudio GROMACS Molecular Dynamics module',
    author="""
    Marc van Dijk - VU University - Amsterdam
    Paul Visscher - Zefiros Software (www.zefiros.eu)
    Felipe Zapata - eScience Center (https://www.esciencecenter.nl/)""",
    author_email=['m4.van.dijk@vu.nl', 'f.zapata@esciencecenter.nl'],
    url='https://github.com/MD-Studio/MDStudio_gromacs',
    license='Apache Software License 2.0',
    keywords='MDStudio GROMACS Molecular Dynamics',
    platforms=['Any'],
    packages=find_packages(),
    package_data={'mdstudio_gromacs': ['data/*', 'schemas/endpoints/*', 'scripts/*']},
    py_modules=[distribution_name],
    scripts=['mdstudio_gromacs/scripts/getEnergies.py'],
    install_requires=['cerise_client', 'mdstudio', 'numpy', 'pyparsing', 'panedr', 'retrying', 'six', 'docker',
                      'twisted==22.1.0'],
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Chemistry',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
)
