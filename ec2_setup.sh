#!/bin/bash

# update package
sudo apt update

# install prerequisites
sudo apt install -y unzip python3-pip sqlite3 python3.12-venv

cd /home/ubuntu

# download and install aws cli
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# copy requirements file from s3 bucket
aws s3 cp s3://<source-bucket>/ec2/requirements.txt /home/ubuntu/requirements.txt   # your source bucket name

# create virtual environment and activate
python3 -m venv airflow_venv
source /home/ubuntu/airflow_venv/bin/activate

# install dependency
sudo apt-get -y install libpq-dev

# install required python packages
pip install -r /home/ubuntu/requirements.txt

# initialize airflow database
airflow db init

# install PostgreSQL
sudo apt-get install -y postgresql postgresql-contrib


# connect to PostgreSQL
sudo -i -u postgres
psql

# backend database command
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
\c airflow;
GRANT ALL ON SCHEMA public TO airflow;

# exit
\q
exit


cd airflow/


# change config in file
# ---------------------

# change backend database to PostgreSQL
# postgresql+psycopg2://<user>:<password>@<host>/<db>
sed -i 's#sqlite:////home/ubuntu/airflow/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg

# change executor type
sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg

# disable example dags
sed -i 's#load_examples = True#load_examples = False#g' airflow.cfg

# enable remote logging
sed -i 's#remote_logging = False#remote_logging = True#g' airflow.cfg

# insert conn_id
sed -i 's#remote_log_conn_id = #remote_log_conn_id = aws_default#g' airflow.cfg

# insert log location
sed -i 's#remote_base_log_folder = #remote_base_log_folder = s3://<log-bucket>/airflow-log/#g' airflow.cfg  # your log bucket name


# re-initialize airflow database again and create airflow user
airflow db init
airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com -p airflow


# add airflow connection
# ----------------------

# delete existing AWS connection
airflow connections delete aws_default

# add new AWS connection
airflow connections add aws_default \
    --conn-type aws \
    --conn-extra '{"region_name": "<region>"}'  # your region name

# add RDS connection
# insert
#     your RDS endpoint
#     your RDS database name
#     your RDS username
#     your RDS password
airflow connections add rds_conn \
    --conn-type postgres \
    --conn-host <rds-endpoint> \    
    --conn-schema <database> \      
    --conn-login <username> \       
    --conn-password <password> \    
    --conn-port 5432

# add Redshift connection
# insert
#     your Redshift workgroup endpoint
#     your Redshift database name
#     your Redshift username
#     your Redshift password
#     your Redshift workgroup name
#     your Redshift region name
airflow connections add redshift_conn \
    --conn-type redshift \
    --conn-host <redshift-workgroup-endpoint> \
    --conn-schema <database> \
    --conn-login <username> \
    --conn-password <password> \
    --conn-port 5439 \
    --conn-extra '''{
        "is_serverless": true,
        "serverless_work_group": "<redshift-workgroup-name>",
        "region": "<region>"
    }'''


# setup airflow variables
# -----------------------

airflow variables set source_bucket_name <source-bucket>        # your source bucket name
airflow variables set raw_bucket_name <raw-bucket>              # your raw bucket name
airflow variables set processed_bucket_name <processed-bucket>  # your processed bucket name
airflow variables set log_bucket_name <log-bucket>              # your log bucket name
airflow variables set emr_runtime_role_arn <emr-role-arn>       # your EMR runtime role ARN
airflow variables set redshift_role_arn <redshift-role-arn>     # your Redshift role ARN