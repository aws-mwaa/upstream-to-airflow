# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

SQS_QUEUE_NAME = 'example_sns_dag_queue'
TOPIC_NAME = 'example_sns_dag_topic'


@task(task_id="create_topic")
def create_sns_topic():
    sns_client = SnsHook().get_conn()
    return sns_client.create_topic(Name=TOPIC_NAME)['TopicArn']


@task(task_id="create_queue")
def create_sqs_queue():
    sqs_client = SqsHook().get_conn()
    return sqs_client.create_queue(queue_name=SQS_QUEUE_NAME)['QueueUrl']


@task(task_id="subscribe_queue")
def subscribe_sqs_queue_to_sns_topic():
    sqs_client = SqsHook().get_conn()
    return sqs_client.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=sqs_queue_url,
        ReturnSubscriptionArn=True,
    )['SubscriptionArn']


@task(task_id="receive_messages")
def await_messages():
    sqs_client = SqsHook().get_conn()
    result = []
    while len(result) == 0:
        result = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            WaitTimeSeconds=60 * 5,
        )['Messages']
    return result


@task(task_id="delete_queue")
def delete_sqs_queue():
    sqs_client = SqsHook().get_conn()
    sqs_client.delete_queue(QueueUrl=sqs_queue_url)


@task(task_id="delete_topic")
def delete_sns_topic():
    sns_client = SnsHook().get_conn()
    sns_client.delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id='example_sns_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    topic_arn = create_sns_topic()
    sqs_queue_url = create_sqs_queue()
    subscriptionArn = subscribe_sqs_queue_to_sns_topic()

    # [START howto_operator_sns_publish_operator]
    publish = SnsPublishOperator(
        task_id='publish',
        target_arn=topic_arn,
        message=f'bar {datetime.now()}',
    )
    # [END howto_operator_sns_publish_operator]

    messages = await_messages()

    delete_sqs_queue()
    delete_sns_topic()
