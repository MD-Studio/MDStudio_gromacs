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
from os.path import (abspath, join)
from tempfile import mktemp

from lie_md.cerise_interface import (
    call_async_cerise_gromit, call_cerise_gromit, create_cerise_config, query_simulation_results)
from lie_md.md_config import set_gromacs_input
from mdstudio.api.endpoint import endpoint
from mdstudio.component.session import ComponentSession
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value


class MDWampApi(ComponentSession):
    """
    Molecular dynamics WAMP methods.
    """
    def authorize_request(self, uri, claims):
        return True

    @endpoint('async_liemd_ligand', 'liemd_ligand_request', 'liemd_response')
    def run_async_ligand_solvent_md(self, request, claims):
        """
        Run Gromacs MD of ligand in solvent. Invoke a ligand solvent simulation
        and returns inmediately, returning to the  caller information for querying the results.

        TODO: Stil requires the protein topology and positional restraint
              (include) files. Makes no sense for ligand in solvent but
              required by gromit somehow.
        """
        # Protein structure not needed. Explicitly set to None
        request['protein_file'] = None

        output = yield self.run_async_gromacs_liemd(request, claims)
        return_value(output)

    @endpoint('async_liemd_protein', 'liemd_protein_request', 'liemd_response')
    def run_async_protein_protein_md(self, request, claims):
        """Run asynchronous Gromacs MD of a protein-ligand system in solvent"""
        output = yield self.run_async_gromacs_liemd(request, claims)
        return_value(output)

    @endpoint('query_liemd_results', 'query_liemd_results_request',
              'query_liemd_results_response')
    def query_liemd_results(self, request, claims):
        """
        Check the status of the simulation and return the results if available.
        """
        clean_remote = request['clean_remote_workdir']
        output = yield query_simulation_results(
            request, self.db, clean_remote=clean_remote)
        return_value(output)

    @endpoint('liemd_ligand', 'liemd_ligand_request', 'liemd_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_ligand_solvent_md(self, request, claims):
        """
        Run Gromacs MD of ligand in solvent

        TODO: Stil requires the protein topology and positional restraint
              (include) files. Makes no sense for ligand in solvent but
              required by gromit somehow.
        """
        # Protein structure not needed. Explicitly set to None
        request['protein_file'] = None

        output = yield self.run_gromacs_liemd(request, claims)
        return_value(output)

    @endpoint('liemd_protein', 'liemd_protein_request', 'liemd_response',
              options=RegisterOptions(invoke='roundrobin'))
    def run_ligand_protein_md(self, request, claims):
        """
        Run Gromacs MD of a protein-ligand system in solvent
        """

        output = yield self.run_gromacs_liemd(request, claims)
        return_value(output)

    @chainable
    def run_gromacs_liemd(self, request, claims):
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

        # Run the MD and retrieve the energies
        output = call_cerise_gromit(gromacs_config, cerise_config, self.db,
                                    clean_remote=request.get('clean_remote_workdir', True))

        if output is None:
            output = {'status': 'failed'}
        else:
            output['status'] = 'completed'

        return_value(output)

    @chainable
    def run_async_gromacs_liemd(self, request, claims):
        """
        async version of the `run_gromacs_liemd` function.
        """
        cerise_config, gromacs_config = self.setup_environment(request)

        output = yield call_async_cerise_gromit(
            gromacs_config, cerise_config, self.db, clean_remote=True)

        return_value(output)

    def setup_environment(self, request):
        """
        Set all the configuration to perform a simulation.
        """
        # Base workdir needs to exist. Might be shared between docker and host
        check_workdir(request['workdir'])

        task_id = uuid.uuid1().hex
        request.update({"task_id": task_id})
        self.log.info("starting liemd task_id: {}".format(task_id))

        task_workdir = create_task_workdir(request['workdir'])

        request['workdir'] = task_workdir
        self.log.info("store output in: {0}".format(task_workdir))

        # Copy input files to task workdir
        request = copy_file_path_objects_to_workdir(request.copy())

        # # Build 'include' file list for cerise/CWL
        # request['include'] = []
        # for file_type in ('attype_itp', 'protein_posre_itp'):
        #     request['include'].append(request[file_type])

        # Load GROMACS configuration
        gromacs_config = set_gromacs_input(request)

        # Load Cerise configuration
        cerise_config = create_cerise_config(request)
        cerise_config['task_id'] = task_id

        with open(join(request['workdir'], "cerise.json"), "w") as f:
            json.dump(cerise_config, f)

        return cerise_config, gromacs_config


def create_task_workdir(workdir):
    """
    Create a task specific directory in workdir based on a unique tmp name
    """
    print("workdir: ", workdir)
    task_workdir = os.path.join(workdir, os.path.basename(mktemp()))
    try:
        os.mkdir(task_workdir)
        return task_workdir
    except:
        raise IOError('Unable to create task directory: {0}'.format(task_workdir))


def check_workdir(workdir):
    """
    Check if a workdir exists
    """
    workdir = abspath(workdir)
    if not os.path.exists(workdir):
        raise IOError('Workdir does not exist: {0}'.workdir)


def copy_file_path_objects_to_workdir(d):
    """
    Copy the serialized files to the local workdir.
    """

    # Check if d is path_file object
    path_file = {'content', 'path', 'extension', 'encoding'}

    def condition(x):
        return isinstance(x, dict) and set(x.keys()).issubset(path_file)

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
    new_path = join(workdir, os.path.basename(file_path))

    if serialized_file['content'] is not None:
        with open(new_path,  'w') as f:
            f.write(serialized_file['content'])
    else:
        shutil.copy(file_path, workdir)

    return new_path
