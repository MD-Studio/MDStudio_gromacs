from mdstudio.deferred.chainable import chainable
from mdstudio.component.session import ComponentSession
from mdstudio.runner import main
from os.path import join
import os


file_path = os.path.realpath(__file__)
root = os.path.split(file_path)[0]


def create_path_file_obj(path):
    """
    Encode the input files
    """
    extension = os.path.splitext(path)[1]

    return {
        u'path': path, u'content': None,
        u'extension': extension}


residues = [28, 29, 65, 73, 74, 75, 76, 78]
workdir = "/tmp/mdstudio/lie_md"
if not os.path.exists(workdir):
    os.makedirs(workdir)

cerise_file = create_path_file_obj(join(root, "cerise_config_binac.json"))
ligand_file = create_path_file_obj(join(root, "compound.pdb"))
protein_file = None
protein_top = create_path_file_obj(join(root, "protein.top"))
topology_file = create_path_file_obj(join(root, "input_GMX.itp"))
include = [create_path_file_obj(join(root, "attype.itp")),
           create_path_file_obj(join(root, "ref_conf_1-posre.itp"))]


class Run_md(ComponentSession):

    def authorize_request(self, uri, claims):
        return True

    @chainable
    def on_run(self):

        result = yield self.call(
            "mdgroup.lie_md.endpoint.liemd_ligand",
            {"cerise_file": cerise_file,
             "ligand_file": ligand_file,
             "protein_file": None,
             "protein_top": protein_top,
             "topology_file": topology_file,
             "include": include,
             "workdir": workdir,
             "parameters": {
                 "sim_time": 0.001,
                 "residues": residues}})

        print("MD results ", result)


if __name__ == "__main__":
    main(Run_md)
