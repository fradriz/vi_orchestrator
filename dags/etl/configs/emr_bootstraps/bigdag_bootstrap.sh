# usage: sh emr_boostrap.sh <branch_name>
BRANCH_NAME=$1
if [ -z "$BRANCH_NAME" ]
then
      DEFAULT_BRANCH_NAME="ANY"
      BRANCH_NAME=$DEFAULT_BRANCH_NAME
      echo "No branch name was passed, using default branch '$BRANCH_NAME'"
else
      echo "Using branch name '$BRANCH_NAME'"
fi

# Randomizing the starting time so all the downloads from the master & nodes are not at the very same moment.
sleep $((RANDOM % 20))

sudo python3 -m pip install --upgrade pip setuptools wheel boto3 jupyter area
sudo yum -y install git-core
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar -P /home/hadoop/

echo "Getting the gitHub token from the SecretsManager"
GITHUB_TOKEN=$(aws secretsmanager get-secret-value --secret-id githubMasterToken --query SecretString --output text | cut -d: -f2 | tr -d \"\})

clone_repo()
{
  REPO=$1
  BRANCH=$2
  REPOS_URL="https://$GITHUB_TOKEN@github.com/dotbc/"
  CMD="git clone $REPOS_URL$REPO.git --single-branch --branch $BRANCH"
  cd ~ || exit
  # Executing and retrying three times if there is an error in cloning
  RETRIES=3
  n=1
  until [ $n -ge $((RETRIES+1)) ]
  do
    date
    echo "Attempt number: $n"
    echo "$CMD"
    eval "$CMD"
    ERROR_CODE=$?
    if [ $ERROR_CODE -eq 0 ]
    then
      break # Success
    else
      echo "ERROR running: $CMD"
      if [ $n -eq $RETRIES ]
      then
        echo "Error Code: $ERROR_CODE - $RETRIES attempts - Can't run: $CMD"
        exit $ERROR_CODE        # exiting with error code <> 0
      fi
    fi

    sleep $((RANDOM % 10))
    sleep $((3**n))         # exponential back-off
    n=$((n+1))
  done
}
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true

echo "Cloning source and composite-etl repos"
clone_repo "cwr_etl" "$BRANCH_NAME"
clone_repo "composite-etl" "$BRANCH_NAME"
export PATH=$PATH:/usr/local/bin:/home/hadoop/.local/bin
pip install -r /home/hadoop/cwr_etl/requirements.txt

cd /home/hadoop/cwr_etl && python3 -m pip install --user .
