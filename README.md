# MDStudio GROMACS

[![Build Status](https://travis-ci.org/MD-Studio/MDStudio_gromacs.svg?branch=master)](https://travis-ci.com/MD-Studio/MDStudio_gromacs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/697c033fd7674ecea28c089150a25dfa)](https://www.codacy.com/app/marcvdijk/MDStudio_gromacs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MD-Studio/MDStudio_gromacs&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/MD-Studio/MDStudio_gromacs/branch/master/graph/badge.svg)](https://codecov.io/gh/MD-Studio/MDStudio_gromacs)

![Configuration settings](mdstudio-logo.png)

mdstudio_gromacs is a python library to call GROMACS using the [cerise](https://github.com/MD-Studio/cerise) package.
This library contains a set of [CWL](https://www.commonwl.org/) scripts to perform several preparation steps, together
with the actual MD simulation and some final postprocessing.

### Install option 1 standalone deployment of the service
For a custom installation clone the MDStudio_gromacs GitHub repository and install `mdstudio_gromacs` locally as:

    pip install (-e) mdstudio_gromacs/

Followed by:

    ./entry_point_mdstudio_gromacs.sh
    
or

    export MD_CONFIG_ENVIRONMENTS=dev,docker
    python -u -m mdstudio_gromacs