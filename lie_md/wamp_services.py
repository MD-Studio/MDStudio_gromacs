# -*- coding: utf-8 -*-

"""
file: wamp_services.py

WAMP service methods the module exposes.
"""
from lie_md.cerise_interface import (call_cerise_gromit, create_cerise_config)
from lie_md.md_config import set_gromacs_input
from mdstudio.api.endpoint import endpoint
from mdstudio.component.session import ComponentSession
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value
from os.path import (abspath, join)
import json
import os
import shutil


class MDWampApi(ComponentSession):
    """
    Molecular dynamics WAMP methods.
    """
    def authorize_request(self, uri, claims):
        return True

    @endpoint('liemd_ligand', 'liemd_ligand_request', 'liemd_ligand_response')
    def run_ligand_solvent_md(self, request, claims):
        """
        Run Gromacs MD of ligand in solvent

        TODO: Stil requires the protein topology and positional restraint
              (include) files. Makes no sense for ligand in solvent but
              required by gromit somehow.
        """

        # Protein structure not needed. Explicitly set to None
        request['protein_file'] = None

        yield self.run_gromacs_liemd(request, claims)

    @endpoint('liemd_protein', 'liemd_protein_request', 'liemd_protein_response')
    def run_ligand_protein_md(self, request, claims):
        """
        Run Gromacs MD of a protein-ligand system in solvent
        """

        yield self.run_gromacs_liemd(request, claims)

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

        # Workdir needs to exist. Might be shared between docker hand host
        request['workdir'] = abspath(request['workdir'])
        if not os.path.exists(request['workdir']):
            raise IOError('Workdir does not exist: {0}'.format(request['workdir']))

        task_id = self.component_config.session.session_id
        request.update({"task_id": task_id})
        self.log.info("starting liemd task_id:{}".format(task_id))

        request = copy_file_path_objects_to_workdir(request.copy())

        # Load GROMACS configuration
        gromacs_config = set_gromacs_input(request)

        # Load Cerise configuration
        cerise_config = create_cerise_config(request)

        with open(join(request['workdir'], "cerise.json"), "w") as f:
            json.dump(cerise_config, f)

        # Run the MD and retrieve the energies
        output = yield call_cerise_gromit(
            gromacs_config, cerise_config, self.db)

        status = 'failed' if output is None else 'completed'
        return_value(
            {'status': status, 'output': output})


def copy_file_path_objects_to_workdir(d):
    """
    Copy the serialized files to the local workdir.
    """
    def condition(x):
        return isinstance(x, dict) and 'content' in x

    workdir = d['workdir']
    for key, val in d.items():
        if condition(val):
            d[key] = copy_file_to_workdir(val, workdir)
        elif isinstance(val, list) and condition(val[0]):
            d[key] = [copy_file_to_workdir(x, workdir) for x in val]

    return d


def copy_file_to_workdir(serialized_file, workdir):
    """ Dump the serialized file into a local folder"""
    # First try to copy the content
    file_path = serialized_file['path']
    file_name = os.path.split(file_path)[1]
    new_path = join(workdir, file_name)

    if serialized_file['content'] is not None:
        with open(new_path,  'w') as f:
            f.write(serialized_file['content'])
    else:
        shutil.copy(file_path, workdir)

    return new_path
