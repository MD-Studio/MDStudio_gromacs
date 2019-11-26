# -*- coding: utf-8 -*-

import os
import sys

from mdstudio.runner import main

# Try import package
try:
    from mdstudio_gromacs.wamp_services import MDWampApi

except ImportError:

    # Add modules in package to path
    modulepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
    if modulepath not in sys.path:
        sys.path.insert(0, modulepath)

    from mdstudio_gromacs.wamp_services import MDWampApi


if __name__ == '__main__':
    main(MDWampApi)
