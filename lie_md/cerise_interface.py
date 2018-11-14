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
import json
import os
import six
import sys


def create_cerise_config(input_session):
    """
    Creates a Cerise service using the path_to_config
    yaml file, together with the cwl_workflow to run
    and store the meta information in the session.

    :param input_session: Object containing the cerise files.
    :type input_session:  :py:dict

    :returns: Cerise config.
    :rtype: :py:dict
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
def call_cerise_gromit(gromacs_config, cerise_config, cerise_db, clean_remote=True):
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
    srv_data = None
    try:
        # Run Jobs
        srv = create_service(cerise_config)
        srv_data = yield submit_new_job(srv, gromacs_config, cerise_config)
        srv_data['status'] = yield wait_till_running(srv, srv_data['job_name'])

        # Register Job
        register_srv_job(srv_data, cerise_db)

        # extract results
        srv_data = yield extract_simulation_info(srv_data, cerise_config, clean_remote)

        # Update job state in DB
        update_srv_info_at_db(srv_data, cerise_db)
    except:
        print("simulation failed due to: ", sys.exc_info()[0])

    finally:
        # Shutdown Service if there are no other jobs running
        if srv_data is not None:
            yield try_to_close_service(srv_data)

    output = {'status': srv_data['status'], 'task_id': srv_data,
              'results': serialize_files(srv_data['results'])}
    return_value(output)


@chainable
def call_async_cerise_gromit(gromacs_config, cerise_config, cerise_db, clean_remote=True):
    """
    Use cerise to run gromacs in a remote cluster, see:
    http://cerise-client.readthedocs.io/en/latest/
    It returns inmediately and provided the user the information to query for the results.

    :param gromacs_config: gromacs simulation parameters
    :type gromacs_config:  :py:dict
    :param cerise_config:  cerise-client process settings.
    :type cerise_config:   :py:dict
    :param cerise_db:      MongoDB db to store the information related to the
                           Cerise services and jobs.

    :returns:              MD output file paths
    :rtype:                :py:dict
    """
    srv_data = None
    try:
        # Run Jobs
        srv = create_service(cerise_config)
        srv_data = yield submit_new_job(srv, gromacs_config, cerise_config)
        register_srv_job(srv_data, cerise_db)
        srv_data['status'] = 'running'
    except:
        print("simulation failed due to: ", sys.exc_info()[0])

    return_value({'status': 'running', 'task_id': srv_data['task_id']})


def find_data(cerise_db, keyword, value):
    """
    Find an entry in the database using `keyword`
    """


@chainable
def query_simulation_results(request, cerise_db, clean_remote=True):
    """
    Check the status of a given
    """
    results = {}
    task_id = request['task_id']

    try:
        srv_data = cerise_db.find_one('cerise', {'task_id': task_id})
        srv = cc.service_from_dict(srv_data)

        job = srv.get_job_by_name(srv_data['job_name'])
        status = job.state

        # Job is still running
        if any(status.lower() == x for x in ["waiting", "running"]):
            status = 'running'

        # Job done
        elif status.lower() == 'success':
            output = wait_extract_clean(job, srv, request['workdir'], clean_remote)
            results = serialize_files(output)

            status = 'completed'

            # Shutdown Service if there are no other jobs running
            yield try_to_close_service(request)
        # Job fails
        else:
            print("Job {} has FAILED!\nCheck output at: {}".format(
                request['job_name'], request['workdir']))
            status = 'failed'

        return {'status': status, 'task_id': task_id, 'results': results}

    except cc.errors.JobNotFound:
        msg = "Job with configuration:\n{}\nWas not found!".format(request)
        raise RuntimeError(msg)


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
def submit_new_job(srv, gromacs_config, cerise_config):
    """
    Create a new job using the provided `srv` and `cerise_config`.
    The job's input is extracted from the `gromacs_config`.
    """
    print("Creating Cerise-client job")
    job = create_lie_job(srv, gromacs_config, cerise_config)

    # Associate a CWL workflow with the job
    job.set_workflow(cerise_config['cwl_workflow'])
    print("CWL worflow is: {}".format(cerise_config['cwl_workflow']))

    # run the job in   the remote
    print(
        "Running the job in a remote machine using docker: {}".format(
            cerise_config['docker_image']))

    # submit the job and register it
    job.run()

    # Collect data
    srv_data = collect_srv_data(
        cc.service_to_dict(srv), gromacs_config, cerise_config)

    return_value(srv_data)


@chainable
def wait_till_running(srv, job_name):
    """wait until the job is running"""
    job = srv.get_job_by_name(job_name)
    while job.state == 'Waiting':
        sleep(2)

    return_value('Running')


@chainable
def extract_simulation_info(srv_data, cerise_config, clean_remote):
    """
    Wait for a job to finish, if the job is already done
    return the information retrieved from the db.

    :param srv_data:      Cerise service meta-data
    :type srv_data:       :py:dict
    :param cerise_config: Cerise service input data
    :type cerise_config:  :py:dict
    """

    print("Extracting output from: {}".format(cerise_config['workdir']))
    if cc.managed_service_exists(srv_data['name']):
        srv = cc.service_from_dict(srv_data)
        job = srv.get_job_by_name(srv_data['job_name'])
        output = wait_extract_clean(job, srv, cerise_config['workdir'], clean_remote)

        # Update data in the db
        srv_data.update({"results": output})
        srv_data['job_state'] = job.state

    # remove MongoDB object id
    srv_data.pop('_id', None)

    return_value(srv_data)


def wait_extract_clean(job, srv, workdir, clean_remote):
    """
    Wait for the `job` to finish, extract the output and cleanup.
    If the job fails returns None.
    """
    log = join(workdir, 'cerise.log')
    wait_for_job(job, log)
    if job.state == "Success":
        output = get_output(job, workdir)
    else:
        output = None

    # Clean up the job and the service.
    if clean_remote:
        print("removing job: {} from Cerise-client".format(job.id))
        srv.destroy_job(job)

    return output


def collect_srv_data(srv_data, gromacs_config, cerise_config):
    """
    Add all the relevant information for the job and
    service to the service dictionary
    """
    # create a unique ID for the ligand
    srv_data['task_id'] = cerise_config['task_id']
    srv_data['username'] = cerise_config['username']
    srv_data['job_type'] = gromacs_config['job_type']
    srv_data['port'] = cerise_config.get('port', 29593)
    srv_data['workdir'] = cerise_config['workdir']

    return srv_data


def create_lie_job(srv, gromacs_config, cerise_config):
    """
    Create a Cerise job using the cerise `srv` and set gromacs
    parameters using `gromacs_config`.
    """

    job = try_to_create_job(srv, cerise_config['task_id'])

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


def update_srv_info_at_db(srv_data, cerise_db):
    """
    Update the service-job data store in the `cerise_db`,
    using the new `srv_data` information.
    """
    # Do not try to update id in the db
    if "_id" in srv_data:
        srv_data.pop("_id")
    query = {'task_id': srv_data['task_id']}
    cerise_db.update_one('cerise', query, {"$set": srv_data})


def wait_for_job(job, cerise_log):
    """
    Wait until job is done.
    """
    print("waiting for job")
    while job.is_running():
        sleep(30)

    # Process output
    if job.state != 'Success':
        print('There was an error: {}'.format(job.state))

    print('Cerise log stored at: {}'.format(cerise_log))
    with open(cerise_log, 'w') as f:
        json.dump(job.log, f, indent=2)


@chainable
def try_to_close_service(srv_data):
    """
    Close service it There are no more jobs and
    the service is still running.
    """
    try:
        srv = cc.service_from_dict(srv_data)

        if len(srv.list_jobs()):
            print("Shutting down Cerise-client service")
            cc.stop_managed_service(srv)
            cc.destroy_managed_service(srv)

    except cc.errors.ServiceNotFound:
        print("There is not Cerise Service running")


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


def get_output(job, workdir):
    """
    retrieve output information from the `job`.
    """
    def copy_output_from_remote(file_name, fmt):
        """
        Copy output files to the localhost.
        """
        path = join(workdir, fmt.format(file_name))
        job.outputs[file_name].save_as(path)

        return path

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
    results = {
        key: copy_output_from_remote(key, fmt)
        for key, fmt in file_formats.items() if key in job.outputs}

    return results


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
