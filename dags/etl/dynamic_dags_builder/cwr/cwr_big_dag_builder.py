import sys
import shutil
import fileinput

from etl.configs.enums import ExecutionType, ExecutionEnvironment
from etl.configs.clusters import DATA_PROVIDERS_CONF

"""
Generating DAGs from a template file.
"""

try:
    ENVIRONMENT = sys.argv[1]
    print(f"Environment == {ENVIRONMENT}")
    valid_envs = set(v for k, v in ExecutionEnvironment.__dict__.items() if '__' not in k)
    assert(ENVIRONMENT in valid_envs)

except AssertionError:
    raise ValueError(f"Expecting a valid environment ({valid_envs}) as argument - got '{ENVIRONMENT}'")

except Exception as e:
    raise Exception(f"Valid environment was not received: {e}")

BASE_PATH = 'dags/etl/'
template_file = BASE_PATH + 'dynamic_dags_builder/cwr/templates/big-dag-template.py'
dst_path = BASE_PATH + 'cwr_dags/dynamic_dags'

dag_name = 'big_dag'
# Getting the providers to run in CWR two steps way
providers = [k for k, v in DATA_PROVIDERS_CONF.items() if v['execution_type'] == ExecutionType.fargateEMR]
print(f"Data providers to create DAGs:{providers}")

for provider in providers:
    new_filename = f"{dst_path}/{dag_name}_{provider}.py"
    print(f"Creating a new file from template '{template_file}' -->> '{new_filename}'")
    shutil.copyfile(template_file, new_filename)

    with fileinput.input(new_filename, inplace=True) as file:
        for line in file:
            new_line = (line
                        .replace('$environment', ENVIRONMENT)
                        .replace('$data_provider', provider))
            print(new_line, end='')
