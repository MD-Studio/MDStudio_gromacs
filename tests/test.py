from mdstudio.deferred.chainable import chainable
from mdstudio.component.session import ComponentSession
from mdstudio.runner import main
from os.path import join
import os
import time


def create_path_file_obj(path):
    """
    Encode the input files
    """
    extension = os.path.splitext(path)[1]

    return {
        'path': path, 'content': None, 'extension': extension}


residues = [28, 29, 65, 73, 74, 75, 76, 78]
workdir = "/tmp/mdstudio_gromacs"
if not os.path.exists(workdir):
    os.makedirs(workdir, exist_ok=True)

# path to the current directory
file_path = os.path.realpath(__file__)
root = os.path.split(file_path)[0]

# Path to the input files
cerise_file = join(root, 'files', "cerise_config_das5.json")
ligand_file = join(root, 'files', "compound.pdb")
protein_file = None
protein_top = join(root, 'files', "protein.top")
topology_file = join(root, 'files', "input_GMX.itp")
include = [join(root, 'files', "attype.itp"), join(root, 'files', "ref_conf_1-posre.itp")]


class Run_md(ComponentSession):

    def authorize_request(self, uri, claims):
        return True

    @chainable
    def on_run(self):
        print("running async function!")
        data = yield self.call(
            "mdgroup.mdstudio_gromacs.endpoint.async_gromacs_ligand",
            {"cerise_file": create_path_file_obj(cerise_file),
             "ligand_file": create_path_file_obj(ligand_file),
             "protein_file": None,
             "protein_top": create_path_file_obj(protein_top),
             "topology_file": create_path_file_obj(topology_file),
             "include": list(map(create_path_file_obj, include)),
             "workdir": workdir,
             "clean_remote_workdir": False,
             "parameters": {
                 "sim_time": 0.001,
                 "residues": residues}})
        print("promised job: ", data)

        print("running sequential version")

        output = yield self.call(
            "mdgroup.mdstudio_gromacs.endpoint.gromacs_ligand",
            {"cerise_file": create_path_file_obj(cerise_file),
             "ligand_file": create_path_file_obj(ligand_file),
             "protein_file": None,
             "protein_top": create_path_file_obj(protein_top),
             "topology_file": create_path_file_obj(topology_file),
             "include": list(map(create_path_file_obj, include)),
             "workdir": workdir,
             "clean_remote_workdir": False,
             "parameters": {
                 "sim_time": 0.001,
                 "residues": residues}})

        print("output sequential: ", output)

        output2 = yield self.call(
            "mdgroup.mdstudio_gromacs.endpoint.query_gromacs_results",
            data
        )
        print("MD output async: ", output2)


if __name__ == "__main__":
    main(Run_md)
