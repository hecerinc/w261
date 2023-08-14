#!/usr/bin/env bash

clear

PROJECT_NAME=w261-student
FOLDER=650352661274
IMAGE_VERSION="2.0-debian10"
BQ_JAR="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
AVRO_PACKAGE="org.apache.spark:spark-avro_2.12:3.1.3"
GRAPHFRAMES_PACKAGE="graphframes:graphframes:0.8.2-spark3.1-s_2.12"
BUCKET_HW_DATA="w261-hw-data"
STARTUP_SCRIPT_URL="gs://${BUCKET_HW_DATA}/startup.sh"
SHUTDOWN_SCRIPT_URL="gs://${BUCKET_HW_DATA}/shutdown.sh"
REPO_LOCATION="/home/${USER}/main"
CLUSTER_NAME="w261"
MACHINE_FAMILY="n2"

# Welcome Screen

echo '

Note: Use your up/down keys to read the entire Welcome screen.
      Press Q to exit Welcome screen.

#################################################
#                                               #
#  Welcome to w261 - Machine Learning at Scale  #
#                                               #
#################################################

This script will ensure you have the proper Environment throughout Week 1 - Week 9.
It will configure/install...

...in the FIRST run:
1. Create a new Project with ID: w261-student-####### either using a berkeley.edu or any Google email.

...in the SECOND run:
1. Associate the Project with a Billing Account (Please have this one ready before running this script).
2. Enable Project"s Compute and Dataproc APIs.
3. Create a regional:
  a. Cloud Router
  b. NAT Gateway
4. Enable Google Private Access on the "Default" VPC Network"s corresponding regional subnet.
5. Create a GCS Bucket for HWs 1-5 and Labs Week 1-11.
6. Create your first single-node Dataproc cluster. We ask you to select a termination policy
   to make the most of your credits. AVRO and Bigquery components are required only for one Lab
   on week 7.

...any subsequent run:
1. Re-create the single-node Dataproc cluster.

Please re-run this script the same way:
gsutil cat gs://w261-hw-data/w261_env.sh | bash -euo pipefail

Note: Use your up/down keys to read the entire Welcome screen.
      Press Q to exit Welcome screen.
' | less

clear

echo ""
read -p "Do you want to proceed?: (y/n) " result < /dev/tty

if [[ $result != [yY] ]]
then
  echo "Good-bye..."
  exit 0
fi

cat > ${HOME}/option_selector.py << EOF
#!/usr/bin/env python3

import sys, os

options = sys.argv[1].split(',')
wrong_choice = ''
while(True):
    print(f'{wrong_choice}Please select {options[0]} from the options below:')
    print()
    for i, option in enumerate(options[1:]):
        print(f'[{i + 1}] {option.strip()}')
    print()

    try:
        selection = int(input('Selection: '))
        if 0 < selection < len(options):
            break
    except:
        pass
    wrong_choice = 'Wrong Choice\n'
    os.system('clear')
print(options[selection])
with open('${HOME}/python_selection', '+w') as fh:
    fh.write(options[selection])
EOF

cat > ${HOME}/parse_results.py << EOF
import sys

results = []
for line in sys.stdin:
    id_, name = line.strip().split('\t')
    results.append(f'{id_} {name}')
print(','.join(results))
EOF

# Create new Project if not exists or switch to preferred Project
echo ""
echo "Checking Project..."

project_id=$(gcloud projects list --format "value(project_id)" --filter "name=$PROJECT_NAME")
email=""

if [ -z "${project_id}" ]
then

  echo "Could not locate a Project labeled \"$PROJECT_NAME\""
  echo "It's highly recommended to create a new Project just for w261 class."
  read -p "Do you want to create one?: (y/n) " result < /dev/tty

  if [[ $result == [yY] ]]
  then
    email=$(gcloud auth list --format "value(account)" --filter "status=ACTIVE")
    if [[ $email == *berkeley.edu ]]
    then
      gcloud projects create ${PROJECT_NAME}-${RANDOM}${RANDOM} --name $PROJECT_NAME --folder $FOLDER -q
      echo ""
      echo "Re-run this script to get to the next step."
      echo ""
    else
      read -p "You are not using a berkeley.edu account, do you want to create a project using account $email: (y/n) " proceed < /dev/tty
      if [[ $proceed == [yY] ]]
      then
        gcloud projects create ${PROJECT_NAME}-${RANDOM}${RANDOM} --name $PROJECT_NAME -q
        echo ""
        echo "Re-run this script to get to the next step."
        echo ""
      else
	    clear
        echo ""
        echo "Click on the far right icon in the top Google Cloud blue bar to switch the active account."
        echo "Please re-run the same script to create the project once you switch the account."
	    sleep 7
	    exit 0
      fi
    fi
  else
    exit 0
  fi
fi

clear

# Select Billing Account
echo ""
echo "Checking Billing Account..."
result=$(gcloud beta billing projects describe $project_id --format "value(billingEnabled)" | tr "[:upper:]" "[:lower:]")

if ! $result
then
    billing_accounts=$(gcloud beta billing accounts list --format "value(name,displayName)" --filter "open=true" | python3 ${HOME}/parse_results.py)
    if [ -z "$billing_accounts" ]
    then
      echo "You don't have any valid billing account. Please create one first by visiting:"
      echo "https://console.cloud.google.com/billing/create"
      echo "Then re-run this script."
      exit 0
    fi
    echo "If available, please use the Billing Account for Education."
    python3 ${HOME}/option_selector.py "a billing account, $billing_accounts" < /dev/tty
    billing_account=$(cat ${HOME}/python_selection | xargs | cut -d " " -f1)
    gcloud beta billing projects link $project_id --billing-account $billing_account
    sleep 3
    clear
fi

clear
echo ""
echo "Switching to $project_id"
gcloud config set project $project_id

sleep 3
clear

# Check if Compute and Dataproc APIs are enabled
echo ""
echo "Checking if Environment ready..."
result=$(gcloud services list --enabled --filter "name~compute*" --format "value(name)")

if [ -z "${result}" ]
then
  echo ""
  echo "Compute API not enabled. Enabling..."
  echo "It might take a few minutes..."
  gcloud services enable compute.googleapis.com
  sleep 3
fi

result=$(gcloud services list --enabled --filter "name~dataproc.googleapis*" --format "value(name)")

if [ -z "${result}" ]
then
  echo""
  echo "Dataproc API not enabled. Enabling..."
  gcloud services enable dataproc.googleapis.com
  sleep 3
fi

result=$(gcloud services list --enabled --filter "name~secretmanager.googleapis*" --format "value(name)")

if [ -z "${result}" ]
then
  echo""
  echo "SecretManager API not enabled. Enabling..."
  gcloud services enable secretmanager.googleapis.com
  sleep 3
fi

clear

# Select a region
echo ""
python3 ${HOME}/option_selector.py "a region,us-central1,us-west1,us-west2,us-west3,us-west4,us-east1,us-east4,us-east5,us-south1" < /dev/tty
REGION=$(cat ${HOME}/python_selection)

clear

# Check if router exists
echo ""
echo "Checking Router..."
result=$(gcloud compute routers list --regions $REGION --format "value(name)" --filter "name=router-$REGION")

if [ -z "${result}" ]
then
  echo ""
  echo "Creating Cloud Router in region: $REGION..."
  gcloud compute routers create router-${REGION} \
    --project=${project_id} \
    --region=${REGION} \
    --network=default
  sleep 3
fi

clear

# Check if NAT Gateway exists
echo ""
echo "Checking NAT Gateway..."
result=$(gcloud compute routers nats list --router router-$REGION --region $REGION --format "value(name)")

if [ -z "${result}" ]
then
  echo ""
  echo "Creating NAT Gateway in region: $REGION..."
  gcloud compute routers nats create nat-${REGION} \
  --router=router-${REGION} \
  --nat-all-subnet-ip-ranges \
  --region ${REGION} \
  --auto-allocate-nat-external-ips
  sleep 3
fi

clear

# Check if Private IP Google Access
echo ""
echo "Checking Private Google Access..."
result=$(gcloud compute networks subnets describe default --region $REGION --format "value(privateIpGoogleAccess)" | tr "[:upper:]" "[:lower:]")

if ! $result
then
  echo ""
  echo "Updating Subnet with Private IP Google Access in region: $REGION"
  gcloud compute networks subnets update default \
    --region=${REGION} \
    --enable-private-ip-google-access
    sleep 3
fi

clear

# Check if HW Data Bucket in place
echo ""
echo "Checking GCS Bucket for w261 Workspace..."



result=""
for bucket in $(gsutil ls)
do
    result=$(gsutil label get $bucket | tr "\n" " " | jq -Rr " fromjson? | .w261")
    if [[ -n $result ]]
    then
        if [ "$result" != "null" ]; then
			echo "$bucket" > $HOME/.data_bucket
            break
        fi
    fi
done



if [ -z "${result}" ]
then
  set +e
  while true
  do
    clear
    echo ""
    echo "Please provide a name for your HW Data Bucket"
    read -p "(use only letters, numbers, dashes and underscores): " bucket < /dev/tty
    if gsutil mb gs://${bucket}
    then
      set -e
      gsutil label ch -l w261:hw-data gs://${bucket}
      echo "Doing a one-time load of HW Data to your recently created bucket..."
      gsutil -m cp -r -J gs://${BUCKET_HW_DATA}/notebooks/jupyter/main/* gs://${bucket}/notebooks/jupyter/
      break
    fi
    sleep 3
  done
  bucket="gs://${bucket}/"
  echo "gsutil -m rsync -r -i -J gs://${BUCKET_HW_DATA}/notebooks/jupyter/main/ ${bucket}notebooks/jupyter/" > /home/${USER}/update_bucket.sh
  chmod +x /home/${USER}/update_bucket.sh

else
#   set +e
#   result=$(gsutil ls ${bucket}main/Assignments/HW5/docker/student/data/all-pages-indexed-in/*.gz 2> /dev/null | wc -l)
#   set -e
#   if [ "${result}" != "16" ]
#   then
#     gsutil -m cp -r gs://${BUCKET_HW_DATA}/main/Assignments/HW5/docker/student/data ${bucket}main/Assignments/HW5/docker/student/ 2> /dev/null
#   fi


  # Sync contents from Source GCS Bucket
  read -p "Do you want to sync your Workspace with the W261 Master Repo? Proceed with caution, you could overwrite your work: (y/n) " result < /dev/tty

  if [[ $result == [yY] ]]
  then
    gsutil -m rsync -r -i -J gs://${BUCKET_HW_DATA}/notebooks/jupyter/main/ ${bucket}notebooks/jupyter/
  fi

fi

clear

# Select a termination policy
  clear
  echo ""
  python3 ${HOME}/option_selector.py "a cluster termination time,1h,2h,3h,6h,12h,24h,48h,72h" < /dev/tty
  max_age=$(cat ${HOME}/python_selection)

clear

# Check if cluster running
# staging_bucket=""
result=$(gcloud dataproc clusters list --region $REGION --filter "clusterName=${CLUSTER_NAME}" --format "value(clusterName)")

if [ -z "${result}" ]
then

#   set +e
#   # Get GitHub username from SecretManager
#   username=$(gcloud secrets versions access "latest" --secret=git_username --project $project_id)
#   if [ $? -ne 0 ]
#   then
#     echo ""
#     echo "Could not find GitHub username, please provide"
#     read -p "GitHub Username (i.e. john_doe_91 ): " username < /dev/tty
#     echo "${username}" | gcloud secrets create git_username --data-file=- --project $project_id
#   fi

#   # Get GitHub token from SecretManager
#   token=$(gcloud secrets versions access "latest" --secret=git_token --project $project_id)
#   if [ $? -ne 0 ]
#   then
#     echo ""
#     echo "Could not fing GitHub Personal Token, please provide"
#     read -p "Token: " token < /dev/tty
#     echo "${token}" | gcloud secrets create git_token --data-file=- --project $project_id
#   fi
#   set -e

  # Pull latest updates from main repo
#   if [ ! -d "${REPO_LOCATION}/.git" ]
#   then
#     # Clone Main Repo
#     git clone https://${username}:${token}@github.com/UCB-w261/main.git
#     sleep 5
#   fi

#   clear
#   echo ""
#   echo "Pulling latest updates from Main Repository..."
#   echo ""

#   cd ${REPO_LOCATION}
#   git pull
#   sleep 5
#   cd

#   # Load Notebooks and Python Files to Staging Bucket
#   clear
#   echo ""
#   echo "Loading Notebooks, Bash and Python scripts to Staging bucket."
#   echo "This process takes a few mins, please be patient..."

#   for staging_bucket in $(gsutil ls | grep "gs://dataproc-staging-us")
#   do
#     gsutil -m cp -r ${REPO_LOCATION}/Assignments ${staging_bucket}notebooks/jupyter/ucb-w261-master/ 2> /dev/null
#     gsutil -m cp -r ${REPO_LOCATION}/LiveSessionMaterials/ ${staging_bucket}notebooks/jupyter/ucb-w261-master/ 2> /dev/null
#     echo "" | gsutil cp - ${staging_bucket}notebooks/jupyter/student-workspace/Assignments/HW1/README.md 2> /dev/null
#     echo "" | gsutil cp - ${staging_bucket}notebooks/jupyter/student-workspace/Assignments/HW2/README.md 2> /dev/null
#     echo "" | gsutil cp - ${staging_bucket}notebooks/jupyter/student-workspace/Assignments/HW3/README.md 2> /dev/null
#     echo "" | gsutil cp - ${staging_bucket}notebooks/jupyter/student-workspace/Assignments/HW4/README.md 2> /dev/null
#     echo "" | gsutil cp - ${staging_bucket}notebooks/jupyter/student-workspace/Assignments/HW5/README.md 2> /dev/null
#   done

  # Select if GraphFrames installed
  echo ""
  python3 ${HOME}/option_selector.py "if GraphFrames installed,Y,N" < /dev/tty
  result=$(cat ${HOME}/python_selection)

  dproc_property=""
  if [[ $result == [yY] ]]
  then
    dproc_property="--properties ^#^spark:spark.jars.packages=${GRAPHFRAMES_PACKAGE}"
  fi

  # Select if AVRO installed
  echo ""
  python3 ${HOME}/option_selector.py "if AVRO installed,Y,N" < /dev/tty
  result=$(cat ${HOME}/python_selection)

  if [[ $result == [yY] ]]
  then
    if [ -z "$dproc_property" ]
    then
      dproc_property="--properties ^#^spark:spark.jars.packages=${AVRO_PACKAGE}"
    else
      dproc_property="${dproc_property},${AVRO_PACKAGE}"
    fi

  fi

  # Select if BQ Connector installed
  echo ""
  python3 ${HOME}/option_selector.py "if BigQuery Connector installed,Y,N" < /dev/tty
  result=$(cat ${HOME}/python_selection)

  bq_property=""
  if [[ $result == [yY] ]]
  then
    bq_property="--properties spark:spark.jars=$BQ_JAR"
  fi

  retry=true

  while $retry
  do

    retry=false
    # Select Machine Type
    echo ""
    python3 ${HOME}/option_selector.py "the machine type,e2-standard-4,e2-standard-8,e2-standard-16,e2-standard-32,n1-standard-4,n1-standard-8,n1-standard-16,n1-standard-32,n2-standard-4,n2-standard-8,n2-standard-16,n2-standard-32" < /dev/tty
    machine_type=$(cat ${HOME}/python_selection)

    # Create Cluster
    clear
    echo ""
    echo "Creating Cluster..."
    echo ""

    #   set +e
    #   staging_bucket=$(gsutil ls | grep "dataproc-staging-${REGION}")
    #   set -e

    #   metadata=""
    #   if [ "${staging_bucket}" ]
    #   then
    #     metadata="--metadata staging_bucket=${staging_bucket},shutdown-script-url=${SHUTDOWN_SCRIPT_URL},startup-script-url=${STARTUP_SCRIPT_URL},data_bucket=${bucket}"
    #   fi

    metadata="--metadata shutdown-script-url=${SHUTDOWN_SCRIPT_URL},startup-script-url=${STARTUP_SCRIPT_URL},data_bucket=${bucket}"

    bucket_name=$(echo $bucket | cut -d "/" -f3)

    set +e

    gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway \
    --region ${REGION} \
    --bucket ${bucket_name} \
    --no-address \
    --single-node \
    --master-machine-type ${machine_type} \
    --master-boot-disk-size 500 \
    --image-version ${IMAGE_VERSION} \
    --optional-components JUPYTER \
    --project ${project_id} \
    --max-age $max_age \
    --verbosity error \
    $metadata \
    --properties spark:spark.ui.port=4040 \
    $dproc_property \
    $bq_property

    if [ $? -ne 0 ]
    then
        echo ""
        read -p "Do you want to try with a different type of machine? (y/n) " result < /dev/tty

        if [[ $result == [yY] ]]
        then
          retry=true
          clear
        else
          echo ""
          echo "Good-bye Adios Ciao Sayōnara..."
          exit 0
        fi
    fi
  done
  set -e

else
  DELETE_TS=$(gcloud dataproc clusters describe ${CLUSTER_NAME}     --region ${REGION}     --format json | jq -r ".config.lifecycleConfig.autoDeleteTime")
  echo "You already have a cluster running, currently set to autoterminate at ${DELETE_TS}"
  read -p "Do you want to update the MAX-AGE setting to ${max_age}? (y/n)" result < /dev/tty

  if [[ $result == [Yy] ]]
  then
    set +e
    gcloud dataproc clusters update ${CLUSTER_NAME} --region ${REGION} --max-age ${max_age}
    set -e
  fi
fi

# if [ -z "${staging_bucket}" ]
# then
#   echo ""
#   echo "Deleting the first cluster, since it's not usable the first time for w261..."

#   gcloud dataproc clusters delete w261 --region $REGION --project $project_id

#   echo ""
#   echo "Please run this script again, now that you have a Staging Bucket and that"
#   echo "you are able to load your Notebooks."
#   echo ""

# else

  cluster_json=$(gcloud dataproc clusters describe ${CLUSTER_NAME} \
    --region ${REGION} \
    --format json)
  # staging_bucket=$(echo $cluster_json | jq -r ".config.configBucket")
  jupyter_endpoint=$(echo $cluster_json | jq -r ".config.endpointConfig.httpPorts.JupyterLab")
  zone=$(echo $cluster_json | jq -r ".config.gceClusterConfig.zoneUri" | rev | cut -d "/" -f1 | rev)

  echo ""
  echo "Please click this endpoint to open your Jupyter Lab UI"
  echo "(It might take a couple of minutes to be accessible):"
  echo ""
  echo $jupyter_endpoint
  echo ""
  echo "NOTE: Click the endpoint below to open the Google Cloud Console in the right project,"
  echo "and be able to see your cluster being deployed:"
  echo ""
  echo "https://console.cloud.google.com/dataproc/clusters?referrer=search&project=${project_id}"
  echo ""
  echo "-----------------------------------------------------------------------------"
  echo ""
  echo "SPARK UI"
  echo "Run the following command here in the CloudShell to establish the SSH Tunnel"
  echo "gcloud compute ssh ${CLUSTER_NAME}-m --zone $zone --ssh-flag \"-L 8080:localhost:4040\""
  echo ""
  echo "Click on the \"Web Preview\" button on top of this terminal."
  echo "Then click on \"Preview on Port 8080\"."
  echo "NOTE: In order to be able to see the SPARK UI, you need to have a running Notebook with an active Spark Context."
# fi

rm ${HOME}/option_selector.py ${HOME}/parse_results.py ${HOME}/python_selection
