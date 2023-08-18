import json
import os
import shutil
import fileinput

config_filepath = 'include/config/'
dag_template_filename = 'include/dag-template.py'

dir_path = os.path.dirname(os.path.realpath(__file__))
# print(f"DIR=={dir_path}")

for filename in os.listdir(config_filepath):
    config_file = f"{config_filepath + filename}"
    with open(config_file) as f:
        config = json.load(f)
        # new_filename = dir_path + '/' + config['DagId'] + '.py'
        new_filename = config['DagId'] + '.py'
        print(f"Creating a new file from template '{dag_template_filename}' -->> '{new_filename}'")
        shutil.copyfile(dag_template_filename, new_filename)

        with fileinput.input(new_filename, inplace=True) as file:
            for line in file:
                new_line = line.replace('dag_id', "'" + config['DagId'] + "'")\
                    .replace('scheduletoreplace', config['Schedule'])\
                    .replace('querytoreplace', config['Query'])
                print(new_line, end='')
