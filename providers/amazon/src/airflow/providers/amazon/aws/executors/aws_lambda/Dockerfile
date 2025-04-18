# Use the official AWS Lambda Python 3.9 base image.
FROM public.ecr.aws/lambda/python:3.9

# Upgrade pip.
RUN pip install --no-cache-dir --upgrade pip

## Installing Python Dependencies
# Python dependencies can be installed by providing a requirements.txt.
# If the file is in a different location, use the requirements_path build argument to specify
# the file path.
ARG requirements_path=./requirements.txt
ENV REQUIREMENTS_PATH=$requirements_path

# Uncomment the two lines below to copy the requirements.txt file to the container, and
# install the dependencies.
# COPY --chown=airflow:root $REQUIREMENTS_PATH /opt/airflow/requirements.txt
# RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Variable for the dag directory:
#ENV DAG_DIR=/tmp/dags
ENV DAG_DIR=/opt/airflow/providers/amazon/tests/system/amazon/aws/

# Create a directory for your DAGs in /tmp.
RUN mkdir -p /tmp/dags

# Copy your python_dag.py into /tmp/dags.
COPY python_dag.py ${DAG_DIR}

# Copy your python_dag.py into /tmp/dags.
COPY example_sqs.py ${DAG_DIR}

# Copy your Lambda function code into the Lambda task directory.
COPY app.py ${LAMBDA_TASK_ROOT}/

# Specify the Lambda function handler.
CMD ["app.lambda_handler"]
