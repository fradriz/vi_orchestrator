import ast
import boto3
import base64
import http.client


def build_conf(value: str, key: str) -> str:
    return "{\"" + value + "\":\"" + key + "\"}"


def dict_to_conf(json_conf):
    conf = ''
    for k, v in json_conf.items():
        conf = conf + build_conf(k, v)
    return conf.replace('}{', ',')


def run_airflow_cmd(airflow_cli_cmd: str, mwaa_env_name: str) -> bytes:
    """
    Run a any Airflow CLI command. Eg: '(airflow) dags trigger <dag_id> -c <conf>'

    :param airflow_cli_cmd: Command to run.
    :param mwaa_env_name: Environment name to run the cmd.
    :return: Request response in 'bytes' format
    """
    client = boto3.client('mwaa')
    # get web token
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    headers = {
        'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
        'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", airflow_cli_cmd, headers)

    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)

    return base64.b64decode(mydata['stdout'])
