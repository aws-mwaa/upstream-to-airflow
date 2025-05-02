.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. |executorName| replace:: Lambda
.. |dockerfileLink| replace:: `here <https://github.com/apache/airflow/blob/main/providers/amazon/src/airflow/providers/amazon/aws/executors/aws_lambda/Dockerfile>`__

===================
AWS Lambda Executor
===================

This is an Airflow executor powered by AWS Lambda. Each task that Airflow schedules
for execution is run as an asynchronous Lambda function invocation. The Lambda
function executes the Airflow task. SQS is used to track invocation successes
and failures (asynchronous Lambda invocations do no return an ARN or execution id
that can be used to track execution state) which the Lambda executor monitors.
Some benefits of an executor like this include:

1. Task isolation: No task can be a noisy neighbor for another. Resources are
   isolated to each individual task. Any failures affect only the single task
   running in that Lambda function. No single user can overload the environment
   by triggering too many tasks.
2. Lightweight execution: Lambda functions start quickly and have minimal overhead,
   making this executor ideal for short-running tasks that don't require substantial
   CPU or memory.
3. Cost effective: Compute resources only exist for the lifetime of the Airflow task
   itself. Lambda's pay-per-use pricing model ensures you only pay for actual task
   execution time.
4. Highly scalable: Lambda can scale to handle thousands of concurrent executions
   with no pre-provisioning.

For a quick start guide please see :ref:`here <lambda_setup_guide>`, it will
get you up and running with a basic configuration.

The below sections provide more generic details about configuration, the
provided example Dockerfile and logging.

.. _lambda_config-options:

Config Options
--------------

There are a number of configuration options available, which can either
be set directly in the airflow.cfg file under an "aws_lambda_executor"
section or via environment variables using the
``AIRFLOW__AWS_LAMBDA_EXECUTOR__<OPTION_NAME>`` format, for example
``AIRFLOW__AWS_LAMBDA_EXECUTOR__FUNCTION_NAME = "myLambdaFunction"``. For
more information on how to set these options, see `Setting Configuration
Options <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, Lambda function, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

In the case of conflicts, the order of precedence from lowest to highest is:

1. Load default values for options which have defaults.
2. Load any values explicitly provided through airflow.cfg or
   environment variables. These are checked with Airflow's config
   precedence.

.. note::
   ``exec_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the Lambda Executor, it is passed directly to the Lambda function as the context input. This allows you to provide additional task-specific data or configuration that the Lambda function can access during execution.

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  FUNCTION_NAME - Name or ARN of the AWS Lambda function to invoke. Required.
-  QUEUE_URL - URL of the SQS queue that will receive task execution results. Required.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  FUNCTION_QUALIFIER - Version or alias of the Lambda function to invoke. Defaults to "$LATEST".
-  DEAD_LETTER_QUEUE_URL - URL of the Dead Letter Queue for processing Lambda execution failures. Optional, but highly recommended.
-  CONN_ID - The Airflow connection (i.e. credentials) used by the Lambda
   executor to make API calls to AWS Lambda and SQS. Defaults to "aws_default".
-  REGION_NAME - The name of the AWS Region where AWS Lambda and SQS are configured. Required.
-  CHECK_HEALTH_ON_STARTUP - Whether to check the Lambda Executor health on startup by testing connectivity to required AWS services. Defaults to "True".
-  MAX_INVOKE_ATTEMPTS - The maximum number of times the Lambda Executor should attempt to invoke a function. This refers to instances where the invocation fails (i.e. AWS API failures, throttling, etc). Defaults to "3".

For a more detailed description of available options, including type
hints and examples, see the ``config_templates`` folder in the Amazon
provider package.

.. note::
   ``exec_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the Lambda Executor, it is passed directly to the Lambda function as the context input (event payload). This allows you to provide additional task-specific data or configuration that the Lambda function can access during execution. Any values in ``exec_config`` will be merged with the task key and command information that the executor sends to the Lambda function.

.. _dockerfile_for_lambda_executor:

.. include:: general.rst
  :start-after: .. BEGIN DOCKERFILE
  :end-before: .. END DOCKERFILE


The most secure method is to use IAM roles. When creating a Lambda Function
Definition, you are able to select a execution role. This role needs
permissions to publish messages to the SQS queue and to write to CloudWatchLogs
or S3 if using AWS remote logging and/or using S3 to synchronize dags
(e.g. ``CloudWatchLogsFullAccess`` or ``CloudWatchLogsFullAccessV2``).
The AWS credentials used on the Scheduler need permissions to
describe and invoke Lambda functions as well as to describe and read/delete
SQS messages.

To create a new role for use by Lambda follow the below:

1. Navigate to the IAM page on the AWS console, and from the left hand
   tab, under Access Management, select Roles.
2. On the Roles page, click Create role on the top right hand corner.
3. Under Trusted entity type, select AWS Service.
4. Select Lambda from the drop down under Use case. Click Next.
5. In the Permissions page, select the permissions the role will need,
   depending on the actions it will perform (at least including the
   permissions described above).
   Click Next after selecting all the required permissions.
6. Enter a name for the new role, and an optional description. Review
   the Trusted Entities, and the permissions for the role. Add any tags
   as necessary, and click Create role.

When creating the Lambda Function (see the :ref:`setup guide <lambda_setup_guide>` for more details), select the appropriate
newly created role.

.. include:: general.rst
  :start-after: .. BEGIN DOCKERFILE_AUTH_SECOND_METHOD
  :end-before: .. END DOCKERFILE_AUTH_SECOND_METHOD

Base Image
~~~~~~~~~~

The Docker image is built upon the ``public.ecr.aws/lambda/python`` image.
This base image contains the necessary components for the Lambda function to
run. Apache Airflow is installed into the image via pip.

It is possible to build the image based of the ``apache/airflow:latest``
image and the Lambda runtime included separately (follow steps `here <https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-ric>`__).

.. include:: general.rst
  :start-after: .. BEGIN LOADING_DAGS
  :end-before: .. END LOADING_DAGS

.. _lamba_logging:

.. include:: general.rst
  :start-after: .. BEGIN LOGGING
  :end-before: .. END LOGGING

-  The configuration options for Airflow remote logging should be
   configured on all hosts and containers running Airflow. For example
   the Webserver requires this config so that it can fetch logs from
   the remote location and the Lambda container requires the config so that
   it can upload the logs to the remote location. See
   `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__
   to read more about how to set Airflow configuration via config file
   or environment variable exports.
-  Adding the Airflow remote logging config to Lambda can be done
   in many ways. Some examples include, but are not limited to:

   -  Exported as environment variables directly in the Dockerfile (see
      the Dockerfile section :ref:`above <dockerfile_for_lambda_executor>`)
   -  Updating the ``airflow.cfg`` file or copy/mounting/downloading a
      custom ``airflow.cfg`` in the Dockerfile.
   -  Added in the Lambda Function definition in plain text or via
      `Secrets/System
      Manager <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar.html>`__

-  Remember that you must have credentials configured within the container to be able
   to interact with the remote service for your logs (e.g. S3,
   CloudWatch Logs, etc). This can be done in many ways. Some examples
   include, but are not limited to:

   -  Export credentials into the Dockerfile directly (see the
      Dockerfile section :ref:`above <dockerfile_for_lambda_executor>`)
   -  Configure an Airflow Connection and provide this as the `remote
      logging conn
      id <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#remote-log-conn-id>`__
      (exported into the Lambda Function by any of the means listed above or
      your preferred method). Airflow will then use these credentials
      *specifically* for interacting with your chosen remote logging
      destination.

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, ECS Task containers, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

Lambda Logging
~~~~~~~~~~~~~~~~

Lambda will send logging information to CloudWatch Logs for the Function
invocations themselves. These logs will include the Airflow Task Operator
logging and any other logging that occurs throughout the life of the process
running in the Lambda function. This can be helpful for debugging issues with
remote logging or while testing remote logging configuration.

**Note: These logs will NOT be viewable from the Airflow Webserver UI.**

.. _lambda_setup_guide:


Setting up a Lambda Executor for Apache Airflow
-----------------------------------------------

There are 3 steps involved in getting a Lambda Executor to work in Apache Airflow:

1. Creating a database that Airflow and the tasks running in Lambda Functions can connect to (if using Airflow 2).

2. Creating and configuring a Lambda Function that can run tasks from Airflow and an SQS Queue to write results to.

3. Configuring Airflow to use the Lambda Executor and the database.

There are different options for selecting a database backend. See `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html>`_ for more information about the different options supported by Airflow. The following guide will explain how to set up a PostgreSQL RDS Instance on AWS.

.. include:: general.rst
  :start-after: .. BEGIN DATABASE_CONNECTION
  :end-before: .. END DATABASE_CONNECTION


Creating a Lambda Function
--------------------------

One way to create a Lambda Function that will work with Apache Airflow, you will need a Docker image that is
properly configured. See the :ref:`Dockerfile <dockerfile_for_lambda_executor>` section for instructions on how to do that.

Once the image is built, it needs to be put in a repository where it can be pulled by Lambda. There are multiple ways
to accomplish this. This guide will go over doing this using Amazon Elastic Container Registry (ECR).

.. include:: general.rst
  :start-after: .. BEGIN ECR_STEPS
  :end-before: .. END ECR_STEPS

Create SQS Queue
~~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the Amazon SQS Service.

2. Click "Create queue".

3. Select "Standard" as the queue type.

4. Provide a name for the queue, and select the defaults that work for your use case.

2. Click Create.

Create Function
~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the AWS Lambda Service.

2. Click "Create Function".

3. Select "Container image" as the function type.

4. Provide a name for the function, select the ECR repository and image tag that you created in the previous steps, and
   select the execution role that you created for use in the Lambda Function.

5. Click Create.

6. Once created, add the following environment variables to the container under Configuration > Environment variables:

- ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``, with the value being the PostgreSQL connection string in the following format using the values set during the `Database section <#create-the-rds-db-instance>`_ above:

.. code-block:: bash

   postgresql+psycopg2://<username>:<password>@<endpoint>/<database_name>


- ``AIRFLOW__LAMBDA_EXECUTOR__QUEUE_URL``, with the value being a comma separated list of security group IDs associated with the VPC used for the RDS instance.

7. Add other configuration as necessary for Airflow generally (see `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__), the Lambda executor (see :ref:`here <lambda_config-options>`) or for remote logging (see :ref:`here <lamba_logging>`). Note that any configuration changes should be made across the entire Airflow environment to keep configuration consistent.

8. If using Airflow 2 allow access to the database. There are many possible methods, but one simple approach is to add a connection to the RDS database via Configuration > RDS databases, follow the steps of the wizard.

Configure Airflow
~~~~~~~~~~~~~~~~~

To configure Airflow to utilize the Lambda Executor and leverage the resources we've set up, create a script (e.g., ``lambda_executor_config.sh``) with at least the following contents:

.. code-block:: bash

   export AIRFLOW__CORE__EXECUTOR='airflow.providers.amazon.aws.executors.aws_lambda.AwsLambdaExecutor'

   export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=<postgres-connection-string>

   export AIRFLOW__AWS_LAMBDA_EXECUTOR__FUNCTION_NAME=<lambda-function-name>

   export AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL=<sqs_queue_url>


.. include:: general.rst
  :start-after: .. BEGIN INIT_DB
  :end-before: .. END INIT_DB
