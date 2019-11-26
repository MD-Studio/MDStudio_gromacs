# -*- coding: utf-8 -*-

"""
file: wamp_services.py

WAMP service methods the module exposes.
"""

import json
import os
import shutil
import uuid

from autobahn.wamp import RegisterOptions
from tempfile import mktemp

from mdstudio.api.endpoint import endpoint
from mdstudio.component.session import ComponentSession
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value

from mdstudio_gromacs.cerise_interface import (call_async_cerise_gromit, call_cerise_gromit, create_cerise_config,
                                               query_simulation_results)
from mdstudio_gromacs.md_config import set_gromacs_input


class MDWampApi(ComponentSession):
    """
    Molecular dynamics WAMP methods.
    """

    def authorize_request(self, uri, claims):
        return True

    @endpoint('query_gromacs_results', 'query_gromacs_results_request', 'async_gromacs_response',
              options=RegisterOptions(invoke='roundrobin'))
    def query_gromacs_results(self, request, claims):
        """
        Check the status of the simulation and return the results if available.

        The request should at least contain a task_id stored in the cerise job DB.
        The response is a typical async_gromacs response, a 'Future' object.
        """

        output = yield query_simulation_results(request, self.db)
        for key, value in request.items():
            if key not in output:
                output[key] = value

        return_value(output)

    @endpoint('async_gromacs_ligand', 'async_gromacs_ligand_request', 'async_gromacs_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_async_ligand_solvent_md(self, request, claims):
        """
        Run Gromacs MD of ligand in solvent. Invoke a ligand solvent simulation
        and returns immediate, returning to the  caller information for querying the results.

        TODO: Still requires the protein topology and positional restraint
              (include) files. Makes no sense for ligand in solvent but
              required by gromit somehow.
        """
        # Protein structure not needed. Explicitly set to None
        request['protein_file'] = None

        output = yield self.run_async_gromacs_gromacs(request, claims)
        return_value(output)

    @endpoint('async_gromacs_protein', 'async_gromacs_protein_request', 'async_gromacs_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_async_protein_protein_md(self, request, claims):
        """
        Run asynchronous Gromacs MD of a protein-ligand system in solvent
        """

        output = yield self.run_async_gromacs_gromacs(request, claims)
        return_value(output)

    @endpoint('gromacs_ligand', 'gromacs_ligand_request', 'gromacs_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_ligand_solvent_md(self, request, claims):
        """
        Run Gromacs MD of ligand in solvent

        TODO: Still requires the protein topology and positional restraint
              (include) files. Makes no sense for ligand in solvent but
              required by gromit somehow.
        """
        # Protein structure not needed. Explicitly set to None
        request['protein_file'] = None

        output = yield self.run_gromacs_gromacs(request, claims)
        return_value(output)

    @endpoint('gromacs_protein', 'gromacs_protein_request', 'gromacs_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_ligand_protein_md(self, request, claims):
        """
        Run Gromacs MD of a protein-ligand system in solvent
        """

        output = yield self.run_gromacs_gromacs(request, claims)
        return_value(output)

    @chainable
    def run_gromacs_gromacs(self, request, claims):
        """
        First it calls gromit to compute the Ligand-solute energies, then
        calls gromit to calculate the protein-ligand energies.

        The Cerise-client infrastructure is used to perform the computations
        in a remote server, see:
        http://cerise-client.readthedocs.io/en/master/index.html

        This function expects the following keywords files to call gromit:
            * cerise_file
            * protein_file (optional)
            * protein_top
            * ligand_file
            * topology_file
            * residues

        The cerise_file is the path to the file containing the configuration
        information required to start a Cerise service.

        Further include files (e.g. *itp files) can be included as a list:
        include=[atom_types.itp, another_itp.itp]

        To perform the energy decomposition a list of the numerical residues
        identifiers is expected, for example:
        residues=[1, 5, 7, 8]

        Note: the protein_file arguments is optional if you do not provide it
        the method will perform a SOLVENT LIGAND MD if you provide the
        `protein_file` it will perform a PROTEIN-LIGAND MD.
        """
        cerise_config, gromacs_config = self.setup_environment(request)
        cerise_config['clean_remote'] = request.get('clean_remote_workdir', True)

        # Run the MD and retrieve the energies
        output = yield call_cerise_gromit(gromacs_config, cerise_config, self.db)

        return_value(output)

    @chainable
    def run_async_gromacs_gromacs(self, request, claims):
        """
        async version of the `run_gromacs_gromacs` function.
        """
        cerise_config, gromacs_config = self.setup_environment(request)
        cerise_config['clean_remote'] = request.get('clean_remote_workdir', True)

        output = yield call_async_cerise_gromit(gromacs_config, cerise_config, self.db)

        return_value(output)

    def setup_environment(self, request):
        """
        Set all the configuration to perform a simulation.
        """
        # Base workdir needs to exist. Might be shared between docker and host
        check_workdir(request['workdir'])

        task_id = uuid.uuid1().hex
        request.update({"task_id": task_id})
        self.log.info("starting gromacs task_id: {}".format(task_id))

        task_workdir = create_task_workdir(request['workdir'])

        request['workdir'] = task_workdir
        self.log.info("store output in: {0}".format(task_workdir))

        # Copy input files to task workdir
        request = copy_file_path_objects_to_workdir(request.copy())

        # Build 'include' file list for cerise/CWL
        request['include'] = []
        for file_type in ('attype_itp', 'protein_posre_itp'):
            request['include'].append(request[file_type])

        # Load GROMACS configuration
        gromacs_config = set_gromacs_input(request)

        # Load Cerise configuration
        cerise_config = create_cerise_config(request)
        cerise_config['task_id'] = task_id

        with open(os.path.join(request['workdir'], "cerise.json"), "w") as f:
            json.dump(cerise_config, f)

        return cerise_config, gromacs_config


def create_task_workdir(workdir):
    """
    Create a task specific directory in workdir based on a unique tmp name
    """

    task_workdir = os.path.join(workdir, os.path.basename(mktemp()))
    try:
        os.mkdir(task_workdir)
        return task_workdir
    except Exception:
        raise IOError('Unable to create task directory: {0}'.format(task_workdir))


def check_workdir(workdir):
    """
    Check if a workdir exists
    """
    workdir = os.path.abspath(workdir)
    if not os.path.exists(workdir):
        raise IOError('Workdir does not exist: {0}'.format(workdir))


def copy_file_path_objects_to_workdir(d):
    """
    Copy the serialized files to the local workdir.
    """

    # Check if d is path_file object
    path_file = {'content', 'path', 'extension', 'encoding'}

    def condition(y):
        return isinstance(y, dict) and set(y.keys()).issubset(path_file)

    workdir = d['workdir']
    for key, val in d.items():
        if condition(val):
            d[key] = copy_file_to_workdir(val, workdir)
        elif isinstance(val, list):
            d[key] = [copy_file_to_workdir(x, workdir) for x in val if condition(x)]

    return d


def copy_file_to_workdir(serialized_file, workdir):
    """
    Dump the serialized file into a local folder
    """

    # First try to copy the content
    file_path = serialized_file['path']
    new_path = os.path.join(workdir, os.path.basename(file_path))

    if serialized_file['content'] is not None:
        with open(new_path,  'w') as f:
            f.write(serialized_file['content'])
    else:
        shutil.copy(file_path, workdir)

    return new_path
