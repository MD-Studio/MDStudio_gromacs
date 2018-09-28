# -*- coding: utf-8 -*-
from __future__ import print_function
from collections import defaultdict
from mdstudio.deferred.chainable import chainable
from mdstudio.deferred.return_value import return_value
from os.path import join
from retrying import retry
from time import sleep
import cerise_client.service as cc
import docker
import hashlib
import json
import os
import six


def create_cerise_config(input_session):
    """
    Creates a Cerise service using the path_to_config
    yaml file, together with the cwl_workflow to run
    and store the meta information in the session.

    :param input_session: Object containing the cerise files.
    :type input_session:  :py:dict

    :returns:             Cerise config.
    :rtype:               :py:dict
    """

    with open(input_session['cerise_file'], 'r') as f:
        config = json.load(f)

    # Return None if key not in dict
    config = defaultdict(lambda: None, config)

    # Set Workflow
    config['cwl_workflow'] = choose_cwl_workflow(input_session['protein_file'])
    config['log'] = join(input_session['workdir'], 'cerise.log')
    config['workdir'] = input_session['workdir']

    return config


@chainable
def call_cerise_gromit(gromacs_config, cerise_config, cerise_db):
    """
    Use cerise to run gromacs in a remote cluster, see:
    http://cerise-client.readthedocs.io/en/latest/

    :param gromacs_config: gromacs simulation parameters
    :type gromacs_config:  :py:dict
    :param cerise_config:  cerise-client process settings.
    :type cerise_config:   :py:dict
    :param cerise_db:      MongoDB db to store the information related to the
                           Cerise services and jobs.

    :returns:              MD output file paths
    :rtype:                :py:dict
    """

    print("Searching for pending jobs in DB")
    srv_data = yield retrieve_service_from_db(cerise_config, gromacs_config, cerise_db)
    if srv_data is None:

        # Create a new service if one is not already running
        print("There are no pending jobs!")
        srv = create_service(cerise_config)
        srv_data = yield submit_new_job(srv, gromacs_config, cerise_config, cerise_db)
        sim_dict = yield extract_simulation_info(srv_data, cerise_config, cerise_db)

    elif srv_data['job_state'] == 'Success':
        print("job is already done!")
        sim_dict = srv_data

    # is the job still running? if it has failed launch it again
    else:
        print("The job status is: ", srv_data['job_state'])
        srv_data = yield restart_srv_job(srv_data, gromacs_config, cerise_config, cerise_db)
        sim_dict = yield extract_simulation_info(srv_data, cerise_config, cerise_db)

    # register job in DB
    yield register_srv_job(srv_data, cerise_db)

    # Shutdown Service if there are no other jobs running
    yield try_to_close_service(srv_data, cerise_db)

    return_value(serialize_files(sim_dict))


def retrieve_service_from_db(cerise_config, gromacs_config, cerise_db):
    """
    Check if there is an alive service in the db.

    :param cerise_config:  Service metadata.
    :param gromacs_config: Path to the ligand geometry.
    :param cerise_db:      Connector to the DB.
    """
    query = {
        'job_type': gromacs_config['job_type'],
        'ligand_id': compute_ligand_id(cerise_config['workdir']),
        'name': cerise_config['docker_name']}

    return cerise_db.find_one('cerise', query)['result']


@retry(wait_random_min=500, wait_random_max=2000)
def create_service(cerise_config):
    """
    Create a Cerise service if one is not already running,
    using the `cerise_config` file.
    """

    try:
        srv = cc.require_managed_service(
                cerise_config['docker_name'],
                cerise_config.get('port', 29593),
                cerise_config['docker_image'],
                cerise_config['username'],
                cerise_config['password'])
        print("Created a new Cerise-client service")
    except docker.errors.APIError as e:
        print(e)
        pass

    return srv


@chainable
def submit_new_job(srv, gromacs_config, cerise_config, cerise_db):
    """
    Create a new job using the provided `srv` and `cerise_config`.
    The job's input is extracted from the `gromacs_config`  and
    the job metadata is stored in the DB using `cerise_db`.
    """

    print("Creating Cerise-client job")
    job = create_lie_job(srv, gromacs_config, cerise_config)

    # Associate a CWL workflow with the job
    job.set_workflow(cerise_config['cwl_workflow'])
    print("CWL worflow is: {}".format(cerise_config['cwl_workflow']))

    # run the job in   the remote
    print(
        "Running the job in a remote machine using docker: {}".format(cerise_config['docker_image']))

    # submit the job and register it
    job.run()

    # Store data in the DB
    srv_data = collect_srv_data(
        job.id, cc.service_to_dict(srv), gromacs_config, cerise_config['username'])

    # wait until the job is running
    while job.state == 'Waiting':
        sleep(2)

    # change jo state
    srv_data['job_state'] = 'Running'

    return_value(srv_data)


@chainable
def restart_srv_job(srv_data, gromacs_config, cerise_config, cerise_db):
    """
    Use a dictionary to restart a Cerise service.

    :param srv_data: Cerise service information.
    :type srv_data:  :py:dict
    """

    job_id = srv_data['job_id']

    try:
        srv = cc.service_from_dict(srv_data)
        cc.start_managed_service(srv)
        srv.get_job_by_id(job_id)
        print("Job {} already running".format(job_id))

    except cc.errors.ServiceNotFound:
        print("There is no cerise service running")
        srv_data = yield start_from_scratch(job_id, gromacs_config, cerise_config, cerise_db)

    except cc.errors.JobNotFound:
        print("There is no job named: {} in the cerise service: {}".format(job_id, srv_data['name']))
        srv_data = yield start_from_scratch(job_id, gromacs_config, cerise_config, cerise_db)

    return_value(srv_data)


@chainable
def start_from_scratch(job_id, gromacs_config, cerise_config, cerise_db):
    """
    If there is not possible to restart a job because it was delete or
    the service is not available, then create a new job and service if
    necessary.
    """

    srv = create_service(cerise_config)
    print("restarting job from scratch")
    yield remove_srv_job_from_db(job_id, cerise_db)
    job = yield submit_new_job(srv, gromacs_config, cerise_config, cerise_db)
    return_value(job)


@chainable
def extract_simulation_info(srv_data, cerise_config, cerise_db):
    """
    Wait for a job to finish, if the job is already done
    return the information retrieved from the db.

    :param srv_data:      Cerise service meta-data
    :type srv_data:       :py:dict
    :param cerise_config: Cerise service input data
    :type cerise_config:  :py:dict
    :param cerise_db:     Mongo db collection to store data related to the
                          Cerise service.
    """

    print("Extracting output from: {}".format(cerise_config['workdir']))
    if cc.managed_service_exists(srv_data['name']):
        srv = cc.service_from_dict(srv_data)
        job = srv.get_job_by_id(srv_data['job_id'])
        output = wait_extract_clean(job, srv, cerise_config, cerise_db)

        # Update data in the db
        srv_data.update({"results": output})
        srv_data['job_state'] = job.state
        update_srv_info_at_db(srv_data, cerise_db)

    # remove MongoDB object id
    srv_data.pop('_id', None)

    return_value(srv_data)


def wait_extract_clean(job, srv, cerise_config, cerise_db):
    """
    Wait for the `job` to finish, extract the output and cleanup.
    If the job fails returns None.
    """

    wait_for_job(job, cerise_config['log'])
    if job.state == "Success":
        output = get_output(job, cerise_config)
    else:
        output = None

    cleanup(job, srv, cerise_db)
    return output


def update_srv_info_at_db(srv_data, cerise_db):
    """
    Update the service-job data store in the `cerise_db`,
    using the new `srv_data` information.
    """

    # Do not try to update id in the db
    if "_id" in srv_data:
        srv_data.pop("_id")
    query = {'ligand_id': srv_data['ligand_id']}
    cerise_db.update_one('cerise', query, {"$set": srv_data})


def collect_srv_data(job_id, srv_data, gromacs_config, username):
    """
    Add all the relevant information for the job and
    service to the service dictionary
    """

    # Save id of the current job in the dict
    srv_data['job_id'] = job_id

    # create a unique ID for the ligand
    srv_data['ligand_id'] = compute_ligand_id(srv_data['workdir'])
    srv_data['username'] = username
    srv_data['job_type'] = gromacs_config['job_type']

    return srv_data


def create_lie_job(srv, gromacs_config, cerise_config):
    """
    Create a Cerise job using the cerise `srv` and set gromacs
    parameters using `gromacs_config`.
    """

    job_name = 'job_{}'.format(cerise_config['task_id'])
    job = try_to_create_job(srv, job_name)
    # job = srv.create_job(job_name)

    # Copy gromacs input files
    job = add_input_files_lie(job, gromacs_config)

    return set_input_parameters_lie(job, gromacs_config)


def try_to_create_job(srv, job_name):
    """
    Create a new job or relaunch cancel or failed job
    """

    try:
        job = srv.get_job_by_name(job_name)
        print("job already exists")
        state = job.state
        if state in ["Waiting", "Running", "Success"]:
            return job
        else:
            srv.destroy_job(job)
            return srv.create_job(job_name)
    except cc.errors.JobNotFound:
        return srv.create_job(job_name)


def add_input_files_lie(job, gromacs_config):
    """
    Tell to Cerise which files are associated to a `job`.
    """

    # Add files to cerise job
    for name in ['protein_top', 'ligand_file', 'topology_file']:
        if name in gromacs_config:
            job.add_input_file(name, gromacs_config[name])

    protein_file = gromacs_config.get('protein_file')
    if protein_file is not None:
        job.add_input_file('protein_file', protein_file)
    else:
        print("Only ligand_file defined, perform SOLVENT-LIGAND MD")

    # Secondary files are all include as part of the protein
    # topology. Just to include them whenever the protein topology
    # is used
    if 'include' in gromacs_config and 'protein_top' in gromacs_config:
        for file_path in gromacs_config['include']:
            job.add_secondary_file('protein_top', file_path)

    return job


def set_input_parameters_lie(job, gromacs_config):
    """
    Set input variables for gromit `job`
    and residues to compute the lie energy.
    """

    # Pass parameters to cerise job
    for k, val in gromacs_config['parameters'].items():
        job.set_input(k, val)

    return job


def register_srv_job(srv_data, cerise_db):
    """
    Register job in the `cerise_db`.
    """
    cerise_db.insert_one('cerise', srv_data)
    print("Added service to mongoDB")


def wait_for_job(job, cerise_log):
    """
    Wait until job is done.
    """

    while job.is_running():
        sleep(30)

    # Process output
    if job.state != 'Success':
        print('There was an error: {}'.format(job.state))

    print('Cerise log stored at: {}'.format(cerise_log))
    with open(cerise_log, 'w') as f:
        json.dump(job.log, f, indent=2)


def cleanup(job, srv, cerise_db, remove_job_from_db=False):
    """
    Clean up the job and the service.
    """

    print("removing job: {} from Cerise-client".format(job.id))
    srv.destroy_job(job)

    # Remove job from DB
    if remove_job_from_db:
        remove_srv_job_from_db(job.id, cerise_db)


def remove_srv_job_from_db(job_id, cerise_db):
    """
    Remove the service and job information from DB
    """

    query = {'job_id': job_id}
    cerise_db.delete_one('cerise', query)
    print('Removed job: {} from database'.format(job_id))


@chainable
def try_to_close_service(srv_data, cerise_db):
    """
    Close service it There are no more jobs and
    the service is still running.

    """
    try:
        srv = cc.service_from_dict(srv_data)

        # Search for other running jobs
        query = {'username': srv_data['username'], 'job_state': 'Running'}
        counts = yield cerise_db.count('cerise', query)
        if counts['total'] == 0:
            print("Shutting down Cerise-client service")
            cc.stop_managed_service(srv)
            cc.destroy_managed_service(srv)

    except cc.errors.ServiceNotFound:
        print("There is not Cerise Service running")
        pass


def serialize_files(data):
    """
    Transform the files to dictionaries representing serialized files
    """

    def serialize(element):
        if isinstance(element, six.string_types):
            if not os.path.isfile(element):
                return element
            else:
                ext = os.path.splitext(element)[-1]
                return {'path': element,
                        'extension': ext.lstrip('.'),
                        'content': None}
        elif isinstance(element, dict):
            return {k: serialize(x) for k, x in element.items()}
        elif isinstance(element, list):
            return [serialize(x) for x in element]
        else:
            return element

    return {key: serialize(val) for key, val in data.items()}


def get_output(job, config):
    """
    retrieve output information from the `job`.
    """

    file_formats = {
        "gromitout": "{}.out",
        "gromiterr": "{}.err",
        "gromacslog2": "{}.out",
        "gromacslog3": "{}.out",
        "gromacslog4": "{}.out",
        "gromacslog5": "{}.out",
        "gromacslog6": "{}.out",
        "gromacslog7": "{}.out",
        "gromacslog8": "{}.out",
        "gromacslog9": "{}.out",
        "energy_edr":  "{}.edr",
        "energy_dataframe": "{}.ene",
        "energyout": "{}.out",
        "energyerr": "{}.err",
        "decompose_dataframe": "{}.ene",
        "decompose_err": "{}.err",
        "decompose_out": "{}.out"}

    # Save all data about the simulation
    outputs = job.outputs
    results = {
        key: copy_output_from_remote(outputs[key], key, config, fmt)
        for key, fmt in file_formats.items() if key in outputs}

    return results


def copy_output_from_remote(file_object, file_name, config, fmt):
    """
    Copy output files to the localhost.
    """

    workdir = config['workdir']

    path = join(workdir, fmt.format(file_name))
    file_object.save_as(path)

    return path


def compute_ligand_id(workdir):
    """
    Use the unique ID of the workdir
    """
    return os.path.split(workdir)[1]


def choose_cwl_workflow(protein_file):
    """
    If there is not a `protein_file`
    perform a solvent-ligand simulation.
    """

    root = os.path.dirname(__file__)
    if protein_file is not None:
        return join(root, 'data/protein_ligand.cwl')
    else:
        return join(root, 'data/solvent_ligand.cwl')
