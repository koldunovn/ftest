import glob
import os
import json
from jinja2 import Environment, FileSystemLoader
import pkg_resources
import argparse
import shutil


templates_path = pkg_resources.resource_filename(__name__,
                                                    f'templates')
file_loader = FileSystemLoader(templates_path)
env = Environment(loader=file_loader)


def sanityhtml():
    parser = argparse.ArgumentParser(prog="sanityhtml",
                                     description="Create html representation for FESOM2 tests.")
    parser.add_argument(
        "--ipath",
        "-i",
        type=str,
        default='./odata/',
        help="Output directory",
    )
    args = parser.parse_args()
# experiment_settings_path = pkg_resources.resource_filename(__name__,
#                                                     f'settings/{experiment_name}/settings.yml')
# homedir = os.path.expanduser('~')
#     machine_settings_path = f'{homedir}/.ftest/machines.yml'

    if not os.path.exists(args.ipath):
        raise FileNotFoundError(f'There is no {args.ipath} directory.')

    experiment_paths = glob.glob(f'{args.ipath}/*')
    experiment_paths.sort()

    experiment_names = []
    for experiment in experiment_paths:
        experiment_names.append(os.path.basename(experiment))

    cn = {}
    cn['title'] = 'FESOM2 sanity checks'
    cn['experiments'] = {}

    for experiment_name, experiment_path in zip(experiment_names, experiment_paths):
        experiment_files = glob.glob(f"{experiment_path}/*")
        experiment_files.sort()
        experiment_last_file = experiment_files[-1]
        with open(experiment_last_file, "r") as read_file:
            test_results = json.load(read_file)

        cn['experiments'][experiment_name] = test_results

    ofile = open('index.html', 'w')
    template = env.get_template('index.jinja')
    output = template.render(cn)
    ofile.write(output)
    ofile.close()

    for experiment_name in experiment_names:
        ofolder = f'./ohtml/{experiment_name}/'
        if not os.path.exists(ofolder):
            os.makedirs(ofolder)
        date = cn['experiments'][experiment_name]['date']
        ofilename = date+".html"
        opath = os.path.join(ofolder, ofilename)
        ofile = open(opath, 'w')
        template = env.get_template('experiment.jinja')
        output = template.render(cn['experiments'][experiment_name])
        ofile.write(output)
        ofile.close()

    if not os.path.exists('./static/'):
        static_path = pkg_resources.resource_filename(__name__,
                                                        f'static')
        shutil.copytree(static_path, './static/')

if __name__ == "__main__":
    # args = parser.parse_args()
    # args.func(args)
    sanityhtml()