import time
from fabric import Connection
from invoke import Responder
from collections import OrderedDict
import os
from datetime import date
import joblib
import yaml
import pkg_resources
import argparse
import json
import glob

finish_codes = [
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL",
    "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "SPECIAL_EXIT", "STOPPED",
    "TIMEOUT"
]

class EnvVarLoader(yaml.SafeLoader):
    pass


def read_yml(yml_path):
    with open(yml_path) as f:
        docs = yaml.load(f, Loader=EnvVarLoader)
    return docs


def connect(machine):
    c = Connection(machine['adress'],
                   user=machine['user'],
                   connect_kwargs={"key_filename": machine['ssh']})
    return c


def record_result(test_results, action, result):
    test_results[action] = OrderedDict()
    if result.ok:
        test_results[action]['status'] = 'OK'
        test_results["latest"] = 'OK'
    else:
        test_results[action]['status'] = 'FAIL'
        test_results["latest"] = 'FAIL'

    test_results[action]['stdout'] = result.stdout
    test_results[action]['stderr'] = result.stderr

    return test_results


def record_no_run(test_results, action):
    test_results[action] = OrderedDict()
    test_results[action]['status'] = 'NOT RUN'
    test_results[action]['stdout'] = 'NOT RUN'
    test_results[action]['stderr'] = 'NOT RUN'
    test_results["latest"] = 'FAIL'
    return test_results


def clone(c, test_results, experiment, login='', passs=''):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'clone')
        return test_results

    repo = experiment['repo']
    branch = experiment['branch']

    result = c.run('mkdir test_today', warn=True, echo=True)
    if result.failed:
        c.run('rm -rf test_today; mkdir test_today', warn=True, echo=True)

    responder_user = Responder(
        pattern=r"Username for",
        response=f"{login}\n",
    )
    responder_pass = Responder(
        pattern=r"Password for",
        response=f"{passs}\n",
    )

    result = c.run(f'module load git; cd test_today;\
                git clone -b {branch} {repo}',
                   echo=True,
                   pty=True,
                   watchers=[responder_user, responder_pass])

    test_results = record_result(test_results, 'clone', result)
    test_results['latest'] = 'OK'

    return test_results


def build(c, test_results):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'build')
        return test_results

    result = c.run('cd test_today/fesom2;\
                    bash -l configure.sh',
                   warn=True,
                   echo=True)
    test_results = record_result(test_results, 'build', result)
    test_results['latest'] = 'OK'

    return test_results


def mkrun(c, test_results, machine, experiment):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'mkrun')
        return test_results

    experiment_name = experiment['experiment_name']
    parent_experiment = experiment['parent_experiment']

    mkrun_line = f"mkrun {experiment_name} {parent_experiment}"

    if 'account' in machine:
        mkrun_line = mkrun_line + " " + f"-a {machine['account']}"

    python_env = machine["python_env"]
    result = c.run(f' {python_env} ;\
                cd test_today/fesom2/;\
                {mkrun_line}',
                   warn=True,
                   echo=True)
    test_results = record_result(test_results, 'mkrun', result)
    test_results['latest'] = 'OK'

    return test_results


def submit(c, test_results, machine, experiment):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'submit')
        return test_results

    experiment_name = experiment['experiment_name']
    machine_name = machine['name']
    work_dir = f"test_today/fesom2/work_{experiment_name}"
    result = c.run(f'cd {work_dir};\
                sbatch job_{machine_name}_new',
                   warn=True,
                   echo=True)

    test_results = record_result(test_results, 'job_submit', result)
    test_results['latest'] = 'OK'
    return test_results


def parce_job_submit(stdout):
    try:
        jobid = stdout.split()[-1]
    except:
        jobid = "FAIL"
    return jobid


def get_status_mistral(c, jobid):
    if jobid == "FAIL":
        status = "NOT RUN"
        return status

    result = c.run(f'sacct -j {jobid}', warn=True, hide=True)
    status = 'UNDEFINED'
    if result.ok:
        lines = []
        for line in result.stdout.splitlines():
            if 'fesom.x' in line:
                print(line.split()[-2])
                status = line.split()[-2]

    return status


def get_status_ollie(c, jobid):
    result = c.run('sudo get_my_jobs.sh -d1', warn=True, hide=True)
    status = "UNDEFINED"
    if result.ok:
        lines = []
        for line in result.stdout.splitlines():
            if jobid in line:
                lines.append(line)
        for line in lines:
            if 'fesom.x' in line:
                print(line.split()[-1])
                status = line.split()[-1]
    return status


def query_status(c, jobid, machine):
    if machine['name'] == 'mistral':
        status = get_status_mistral(c, jobid)
    elif machine['name'] == 'ollie':
        status = get_status_ollie(c, jobid)
    else:
        print(f'Unsupported machine {machine} for query the job status.')
    return status


def exit_status(c, test_results, machine, jobid, sleep=10, attempts=10):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'exit_status')
        return test_results

    status = "UNDEFINED"
    attempt = 0

    while (status not in finish_codes) or (attempt >= attempts):
        time.sleep(sleep)
        status = query_status(c, jobid, machine)
        attempt += 1

    test_results["exit_status"] = {}
    test_results["exit_status"]['status'] = status
    test_results["exit_status"]['stdout'] = " "
    test_results["exit_status"]['stderr'] = " "

    if status == "COMPLETED":
        test_results['latest'] = 'OK'
    else:
        test_results['latest'] = 'FAIL'

    print(status)
    return test_results


def check(c, test_results, machine, experiment):

    if test_results['latest'] != 'OK':
        test_results = record_no_run(test_results, 'fcheck')
        return test_results

    experiment_name = experiment['experiment_name']

    python_env = machine["python_env"]
    tolerance = float(experiment["tolerance"])
    work_dir = f"test_today/fesom2/work_{experiment_name}"
    result = c.run(f'{python_env};\
                cd {work_dir};\
                fcheck . -a {tolerance}',
                   warn=True,
                   echo=True)

    test_results = record_result(test_results, 'fcheck', result)
    test_results['latest'] = 'OK'

    return test_results

def list_experiments():
    settings_path = pkg_resources.resource_filename(__name__,
                                                f'settings/')
    experiment_paths = glob.glob(f'{settings_path}/*')
    experiment_paths.sort()

    for experiment in experiment_paths:
        print(experiment)


def ftest():
    parser = argparse.ArgumentParser(prog="ftest",
                                     description="run FESOM2 tests.")
    parser.add_argument("ename", help="Name of the experiment", default='ollie_sanity', nargs='*')

    parser.add_argument(
        "--opath",
        "-o",
        type=str,
        default='./odata/',
        help="Output directory",
    )
    parser.add_argument(
        "--branch",
        "-b",
        type=str,
        default=None,
        help="test branch",
    )
    parser.add_argument(
        "--list",
        "-l",
        action='store_true'
    )

    args = parser.parse_args()

    if args.list:
        list_experiments()
        exit()

    experiment_name = args.ename[0]

    experiment_settings_path = pkg_resources.resource_filename(__name__,
                                                    f'settings/{experiment_name}/settings.yml')
    homedir = os.path.expanduser('~')
    machine_settings_path = f'{homedir}/.ftest/machines.yml'
    repo_settings_path = f'{homedir}/.ftest/repos.yml'

    machine_settings = read_yml(machine_settings_path)
    repo_settings = read_yml(repo_settings_path)
    experiment = read_yml(experiment_settings_path)
    machine = machine_settings[experiment['machine']]

    if args.branch is not None:
        experiment['branch'] = args.branch

    test_results = OrderedDict()
    test_results['latest'] = 'OK'
    today = date.today().strftime("%Y-%m-%d")
    test_results['date'] = today

    c = connect(machine)
    test_results = clone(c, test_results, experiment,
                         login = repo_settings['login'],
                          passs= repo_settings['pass'])
    test_results = build(c, test_results)
    test_results = mkrun(c, test_results, machine, experiment)
    test_results = submit(c, test_results, machine, experiment)

    jobid = parce_job_submit(test_results['job_submit']['stdout'])

    test_results = exit_status(c,
                            test_results,
                            machine,
                            jobid,
                            sleep=10,
                            attempts=10)
    test_results = check(c, test_results, machine, experiment)

    del test_results['latest']
    c.close()

    ofolder = os.path.join(args.opath , experiment["experiment_name"])

    if not os.path.exists(ofolder):
        os.makedirs(ofolder)

    ofile_name = os.path.join(ofolder, experiment["experiment_name"] + "_" + today+".json")
    with open(ofile_name, 'w') as outfile:
        json.dump(test_results, outfile)

if __name__ == "__main__":
    # args = parser.parse_args()
    # args.func(args)
    ftest()