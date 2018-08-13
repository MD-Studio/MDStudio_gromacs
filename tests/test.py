from mdstudio.deferred.chainable import chainable
from mdstudio.component.session import ComponentSession
from mdstudio.runner import main
from os.path import join
import os
import shutil


def create_path_file_obj(path):
    """
    Encode the input files
    """
    print("serializing: ", path)
    extension = os.path.splitext(path)[1]
    with open(path, 'r') as f:
        content = f.read()

    return {
        'path': path, 'content': content, 'extension': extension}


residues = [28, 29, 65, 73, 74, 75, 76, 78]
workdir = "/tmp/mdstudio/lie_md"
if os.path.exists(workdir):
    shutil.rmtree(workdir)

cerise_file = join(workdir, "cerise_config_das5.json")
ligand_file = join(workdir, "compound.pdb")
protein_file = None
protein_top = join(workdir, "protein.top")
topology_file = join(workdir, "input_GMX.itp")
include = [join(workdir, "attype.itp"), join(workdir, "ref_conf_1-posre.itp")]

shutil.copytree('files', workdir)


class Run_md(ComponentSession):

    def authorize_request(self, uri, claims):
        return True

    @chainable
    def on_run(self):
        r = yield self.call(
            "mdgroup.lie_md.endpoint.liemd",
            {"cerise_file": create_path_file_obj(cerise_file),
             "ligand_file": create_path_file_obj(ligand_file),
             "protein_file": None,
             "protein_top": create_path_file_obj(protein_top),
             "topology_file": create_path_file_obj(topology_file),
             "include": list(map(create_path_file_obj, include)),
             "workdir": workdir,
             "parameters": {
                 "sim_time": 0.001,
                 "residues": residues}})
        print("MD results ", r)


if __name__ == "__main__":
    main(Run_md)
