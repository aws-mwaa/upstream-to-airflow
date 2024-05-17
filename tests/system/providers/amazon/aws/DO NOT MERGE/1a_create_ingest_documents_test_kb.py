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
from __future__ import annotations

import json
import logging
import os.path
import tempfile
from datetime import datetime
from time import sleep
from urllib.request import urlretrieve

import boto3
from botocore.exceptions import ClientError
from opensearchpy import (
    AuthorizationException,
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook
from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateDataSourceOperator,
    BedrockCreateKnowledgeBaseOperator,
    BedrockIngestDataOperator,
    BedrockRaGOperator,
    BedrockRetrieveOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveSensor,
)
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

###########################################################################################################
# NOTE:
#   Access to the following foundation model(s) must be requested via the Amazon Bedrock console and may
#   take up to 24 hours to apply:
###########################################################################################################

CLAUDE_MODEL_ID = "anthropic.claude-v2"
TITAN_MODEL_ID = "amazon.titan-embed-text-v1"

###########################################################################################################
# NOTE:
#   This is a DAG version of the example found on the aws-samples page at
#   https://github.com/aws-samples/amazon-bedrock-samples/blob/main/knowledge-bases/01-rag-concepts/
###########################################################################################################

DAG_ID = "1a_create_ingest_documents_test_kb"
ENV_ID = "demo-1a"

QUERY = "What is Amazon doing in the field of generative AI?"

# TODO  remove the license!!


def await_policies_attached(role_name, policy_names):
    retries = 10
    while retries > 0:
        found_policies = [
            policy["PolicyName"]
            for policy in boto3.client("iam").list_attached_role_policies(RoleName=role_name)[
                "AttachedPolicies"
            ]
        ]
        print(f"Waiting for {policy_names} in {role_name}, found {found_policies}")
        if all([policy in found_policies for policy in policy_names]):
            # NOTE
            #   There may be a race condition with a difference between "existing" and "applied/propagated".
            #   create_vector_index fails at random with an "unauthorized" message.
            return
        sleep(30)
        retries -= 1
    raise Exception("No policies attached.")


@task
def create_bedrock_execution_role():
    fm_policy_name = f"AmazonBedrockFoundationModelPolicyForKnowledgeBase_{ENV_ID}"
    s3_policy_name = f"AmazonBedrockS3PolicyForKnowledgeBase_{ENV_ID}"

    def _create_or_get_iam_policy(name, description, document):
        print(f"Creating IAM Policy {name}.")
        try:
            policy = iam_client.create_policy(
                PolicyName=name,
                PolicyDocument=json.dumps(document),
                Description=description,
            )["Policy"]
            iam_client.get_waiter("policy_exists").wait(PolicyArn=policy["Arn"])
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                policy = next(
                    policy
                    for policy in (iam_client.list_policies()["Policies"])
                    if policy["PolicyName"] == name
                )
                print(f"Policy {name} already exists")
                print(e)
            else:
                raise
        return policy

    def _create_or_get_iam_role(name, description, document):
        print(f"Creating IAM Role {name}.")
        try:
            role = iam_client.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=json.dumps(document),
                Description=description,
                MaxSessionDuration=3600,
            )["Role"]
            iam_client.get_waiter("role_exists").wait(RoleName=name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                role = iam_client.get_role(RoleName=name)["Role"]
                print(f"Role {name} already exists.")
                print(e)
            else:
                raise
        return role

    execution_role = _create_or_get_iam_role(
        name=bedrock_execution_role_name,
        description="Amazon Bedrock Knowledge Base Execution Role for accessing OpenSearchServerless and S3.",
        document={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "bedrock.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
    )

    fm_policy = _create_or_get_iam_policy(
        name=fm_policy_name,
        description="Policy for accessing foundation model.",
        document={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["bedrock:InvokeModel"],
                    "Resource": [f"arn:aws:bedrock:{region_name}::foundation-model/{TITAN_MODEL_ID}"],
                }
            ],
        },
    )
    iam_client.attach_role_policy(RoleName=bedrock_execution_role_name, PolicyArn=fm_policy["Arn"])

    s3_policy = _create_or_get_iam_policy(
        name=s3_policy_name,
        description="Policy for reading documents from s3.",
        document={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"],
                    "Condition": {"StringEquals": {"aws:ResourceAccount": f"{account_id}"}},
                }
            ],
        },
    )
    iam_client.attach_role_policy(RoleName=bedrock_execution_role_name, PolicyArn=s3_policy["Arn"])

    return execution_role["Arn"]


@task
def create_oss_policy_attach_bedrock_execution_role(execution_role_arn):
    oss_policy_name = f"AmazonBedrockOSSPolicyForKnowledgeBase_{ENV_ID}"

    # Define oss policy document
    oss_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["aoss:*"],
                "Resource": [f"arn:aws:aoss:{region_name}:{account_id}:collection/*"],
            }
        ],
    }
    try:
        oss_policy_arn = iam_client.create_policy(
            PolicyName=oss_policy_name,
            PolicyDocument=json.dumps(oss_policy_document),
            Description="Policy for accessing opensearch serverless",
        )["Policy"]["Arn"]

        print("Opensearch serverless arn: ", oss_policy_arn)
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            all_policies = iam_client.list_policies()["Policies"]
            oss_policy_arn = next(
                policy["Arn"] for policy in all_policies if policy["PolicyName"] == oss_policy_name
            )
            print(f"Policy {oss_policy_name} already exists")
            print(e)
        else:
            raise

    role_name = next(
        role["RoleName"] for role in iam_client.list_roles()["Roles"] if role["Arn"] == execution_role_arn
    )
    print(f"attaching policy {oss_policy_arn} to role {role_name}: {oss_policy_document}")
    iam_client.attach_role_policy(RoleName=role_name, PolicyArn=oss_policy_arn)
    await_policies_attached(role_name, [oss_policy_name])


@task(trigger_rule=TriggerRule.ALL_DONE)
def iam_cleanup():
    # Detach and delete all policies
    for arn in [
        policy["PolicyArn"]
        for policy in boto3.client("iam").list_attached_role_policies(RoleName=bedrock_execution_role_name)[
            "AttachedPolicies"
        ]
    ]:
        iam_client.detach_role_policy(RoleName=bedrock_execution_role_name, PolicyArn=arn)
        iam_client.delete_policy(PolicyArn=arn)

    # Delete Role
    iam_client.delete_role(RoleName=bedrock_execution_role_name)


@task
def create_opensearch_policies(bedrock_role_arn: str, collection_name: str, policy_name_suffix: str) -> None:
    """
    Create security, network and data access policies within Amazon OpenSearch Serverless.

    :param bedrock_role_arn: Arn of the Bedrock Knowledge Base Execution Role.
    :param collection_name: Name of the OpenSearch collection to apply the policies to.
    :param policy_name_suffix: EnvironmentID or other unique suffix to append to the policy name.
    """

    encryption_policy_name = f"{naming_prefix}sp-{policy_name_suffix}"
    network_policy_name = f"{naming_prefix}np-{policy_name_suffix}"
    access_policy_name = f"{naming_prefix}ap-{policy_name_suffix}"

    def _create_security_policy(name, policy_type, policy):
        try:
            aoss_client.create_security_policy(name=name, policy=json.dumps(policy), type=policy_type)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                log.info("OpenSearch security policy %s already exists.", name)
            raise

    def _create_access_policy(name, policy_type, policy):
        try:
            aoss_client.create_access_policy(name=name, policy=json.dumps(policy), type=policy_type)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                log.info("OpenSearch data access policy %s already exists.", name)
            raise

    _create_security_policy(
        name=encryption_policy_name,
        policy_type="encryption",
        policy={
            "Rules": [{"Resource": [f"collection/{collection_name}"], "ResourceType": "collection"}],
            "AWSOwnedKey": True,
        },
    )

    _create_security_policy(
        name=network_policy_name,
        policy_type="network",
        policy=[
            {
                "Rules": [{"Resource": [f"collection/{collection_name}"], "ResourceType": "collection"}],
                "AllowFromPublic": True,
            }
        ],
    )

    _create_access_policy(
        name=access_policy_name,
        policy_type="data",
        policy=[
            {
                "Rules": [
                    {
                        "Resource": [f"collection/{collection_name}"],
                        "Permission": [
                            "aoss:CreateCollectionItems",
                            "aoss:DeleteCollectionItems",
                            "aoss:UpdateCollectionItems",
                            "aoss:DescribeCollectionItems",
                        ],
                        "ResourceType": "collection",
                    },
                    {
                        "Resource": [f"index/{collection_name}/*"],
                        "Permission": [
                            "aoss:CreateIndex",
                            "aoss:DeleteIndex",
                            "aoss:UpdateIndex",
                            "aoss:DescribeIndex",
                            "aoss:ReadDocument",
                            "aoss:WriteDocument",
                        ],
                        "ResourceType": "index",
                    },
                ],
                "Principal": [(StsHook().conn.get_caller_identity()["Arn"]), bedrock_role_arn],
            }
        ],
    )


@task
def create_collection(collection_name: str):
    """
    Call the Amazon OpenSearch Serverless API and create a collection with the provided name.

    :param collection_name: The name of the Collection to create.
    """
    log.info("\nCreating collection: %s.", collection_name)
    return aoss_client.create_collection(name=collection_name, type="VECTORSEARCH")["createCollectionDetail"][
        "id"
    ]


@task
def create_vector_index(index_name: str, collection_id: str, region: str):
    """
    Use the OpenSearchPy client to create the vector index for the Amazon Open Search Serverless Collection.

    :param index_name: The vector index name to create.
    :param collection_id: ID of the collection to be indexed.
    :param region: Name of the AWS region the collection resides in.
    """
    # Build the OpenSearch client
    oss_client = OpenSearch(
        hosts=[{"host": f"{collection_id}.{region}.aoss.amazonaws.com", "port": 443}],
        http_auth=AWSV4SignerAuth(boto3.Session().get_credentials(), region, "aoss"),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300,
    )
    index_config = {
        "settings": {
            "index.knn": "true",
            "number_of_shards": 1,
            "knn.algo_param.ef_search": 512,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "vector": {
                    "type": "knn_vector",
                    "dimension": 1536,
                    "method": {"name": "hnsw", "engine": "faiss", "space_type": "l2"},
                },
                "text": {"type": "text"},
                "text-metadata": {"type": "text"},
            }
        },
    }

    retries = 35
    while retries > 0:
        try:
            response = oss_client.indices.create(index=index_name, body=json.dumps(index_config))
            log.info("Creating index: %s.", response)
            break
        except AuthorizationException as e:
            # Index creation can take up to a minute and there is no (apparent?) way to check the current state.
            log.info(
                "Access denied; policy permissions have likely not yet propagated, %s tries remaining.",
                retries,
            )
            log.debug(e)
            retries -= 1
            sleep(2)


@task
def copy_data_to_s3(bucket: str):
    """
    Download some sample data and upload it to 3S.

    :param bucket: Name of the Amazon S3 bucket to send the data to.
    """

    # Monkey patch the list of names available for NamedTempFile so we can pick the names of the downloaded files.
    backup_get_candidate_names = tempfile._get_candidate_names  # type: ignore[attr-defined]
    destinations = iter(
        [
            "AMZN-2022-Shareholder-Letter.pdf",
            "AMZN-2021-Shareholder-Letter.pdf",
            "AMZN-2020-Shareholder-Letter.pdf",
            "AMZN-2019-Shareholder-Letter.pdf",
        ]
    )
    tempfile._get_candidate_names = lambda: destinations  # type: ignore[attr-defined]

    # Download the sample data files, save them as named temp files using the names above, and upload to S3.
    sources = [
        "https://s2.q4cdn.com/299287126/files/doc_financials/2023/ar/2022-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2022/ar/2021-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2021/ar/Amazon-2020-Shareholder-Letter-and-1997-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2020/ar/2019-Shareholder-Letter.pdf",
    ]

    for source in sources:
        with tempfile.NamedTemporaryFile(mode="w", prefix="") as data_file:
            urlretrieve(source, data_file.name)
            S3Hook().conn.upload_file(
                Filename=data_file.name, Bucket=bucket, Key=os.path.basename(data_file.name)
            )

    # Revert the monkey patch.
    tempfile._get_candidate_names = backup_get_candidate_names  # type: ignore[attr-defined]
    # Verify the path reversion worked.
    with tempfile.NamedTemporaryFile(mode="w", prefix=""):
        # If the reversion above did not apply correctly, this will fail with
        # a StopIteration error because the iterator will run out of names.
        ...


@task
def get_collection_arn(collection_id: str):
    """
    Return a collection ARN for a given collection ID.

    :param collection_id: ID of the collection to be indexed.
    """
    return next(
        colxn["arn"]
        for colxn in aoss_client.list_collections()["collectionSummaries"]
        if colxn["id"] == collection_id
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_data_source(knowledge_base_id: str, data_source_id: str):
    """
    Delete the Amazon Bedrock data source created earlier.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto_operator:BedrockDeleteDataSource`

    :param knowledge_base_id: The unique identifier of the knowledge base which the data source is attached to.
    :param data_source_id: The unique identifier of the data source to delete.
    """
    log.info("Deleting data source %s from Knowledge Base %s.", data_source_id, knowledge_base_id)
    bedrock_agent_client.delete_data_source(dataSourceId=data_source_id, knowledgeBaseId=knowledge_base_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_knowledge_base(knowledge_base_id: str):
    """
    Delete the Amazon Bedrock knowledge base created earlier.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:BedrockDeleteKnowledgeBase`

    :param knowledge_base_id: The unique identifier of the knowledge base to delete.
    """
    log.info("Deleting Knowledge Base %s.", knowledge_base_id)
    bedrock_agent_client.delete_knowledge_base(knowledgeBaseId=knowledge_base_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_vector_index(index_name: str, collection_id: str):
    """
    Delete the vector index created earlier.

    :param index_name: The name of the vector index to delete.
    :param collection_id: ID of the collection to be indexed.
    """
    host = f"{collection_id}.{region_name}.aoss.amazonaws.com"
    credentials = boto3.Session().get_credentials()
    awsauth = AWSV4SignerAuth(credentials, region_name, "aoss")

    # Build the OpenSearch client
    oss_client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300,
    )
    oss_client.indices.delete(index=index_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_collection(collection_id: str):
    """
    Delete the OpenSearch collection created earlier.

    :param collection_id: ID of the collection to be indexed.
    """
    log.info("Deleting collection %s.", collection_id)
    aoss_client.delete_collection(id=collection_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_opensearch_policies(collection_name: str):
    """
    Delete the security, network and data access policies created earlier.

    :param collection_name: All policies in the given collection name will be deleted.
    """

    access_policies = aoss_client.list_access_policies(
        type="data", resource=[f"collection/{collection_name}"]
    )["accessPolicySummaries"]
    log.info("Found access policies for %s: %s", collection_name, access_policies)
    if not access_policies:
        raise Exception("No access policies found?")
    for policy in access_policies:
        log.info("Deleting access policy for %s: %s", collection_name, policy["name"])
        aoss_client.delete_access_policy(name=policy["name"], type="data")

    for policy_type in ["encryption", "network"]:
        policies = aoss_client.list_security_policies(
            type=policy_type, resource=[f"collection/{collection_name}"]
        )["securityPolicySummaries"]
        if not policies:
            raise Exception("No security policies found?")
        log.info("Found %s security policies for %s: %s", policy_type, collection_name, policies)
        for policy in policies:
            log.info("Deleting %s security policy for %s: %s", policy_type, collection_name, policy["name"])
            aoss_client.delete_security_policy(name=policy["name"], type=policy_type)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    iam_client = boto3.client("iam")
    aoss_client = OpenSearchServerlessHook(aws_conn_id=None).conn
    bedrock_agent_client = BedrockAgentHook(aws_conn_id=None).conn

    region_name = boto3.session.Session().region_name
    account_id = StsHook().get_account_number()

    naming_prefix = "bedrock-example-"
    bucket_name = f"{naming_prefix}{ENV_ID}"
    index_name = f"{naming_prefix}index-{ENV_ID}"
    knowledge_base_name = f"{naming_prefix}{ENV_ID}"
    vector_store_name = f"{naming_prefix}{ENV_ID}"
    data_source_name = f"{naming_prefix}ds-{ENV_ID}"
    bedrock_execution_role_name = f"AmazonBedrockExecutionRoleForKnowledgeBase_{ENV_ID}"

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)
    create_execution_role = create_bedrock_execution_role()

    create_opensearch_policies = create_opensearch_policies(
        bedrock_role_arn=create_execution_role,
        collection_name=vector_store_name,
        policy_name_suffix=ENV_ID,
    )

    collection = create_collection(collection_name=vector_store_name)

    await_collection = OpenSearchServerlessCollectionActiveSensor(
        task_id="await_collection",
        collection_name=vector_store_name,
    )

    create_knowledge_base = BedrockCreateKnowledgeBaseOperator(
        task_id="create_knowledge_base",
        name=knowledge_base_name,
        embedding_model_arn=f"arn:aws:bedrock:{region_name}::foundation-model/{TITAN_MODEL_ID}",
        role_arn=create_execution_role,
        storage_config={
            "type": "OPENSEARCH_SERVERLESS",
            "opensearchServerlessConfiguration": {
                "collectionArn": get_collection_arn(collection),
                "vectorIndexName": index_name,
                "fieldMapping": {
                    "vectorField": "vector",
                    "textField": "text",
                    "metadataField": "text-metadata",
                },
            },
        },
    )

    create_data_source = BedrockCreateDataSourceOperator(
        task_id="create_data_source",
        knowledge_base_id=create_knowledge_base.output,
        name=data_source_name,
        bucket_name=bucket_name,
    )

    ingest_data = BedrockIngestDataOperator(
        task_id="ingest_data",
        knowledge_base_id=create_knowledge_base.output,
        data_source_id=create_data_source.output,
    )

    knowledge_base_rag = BedrockRaGOperator(
        task_id="knowledge_base_rag",
        input=QUERY,
        source_type="KNOWLEDGE_BASE",
        model_arn=f"arn:aws:bedrock:{region_name}::foundation-model/{CLAUDE_MODEL_ID}",
        knowledge_base_id=create_knowledge_base.output,
    )

    retrieve = BedrockRetrieveOperator(
        task_id="retrieve",
        knowledge_base_id=create_knowledge_base.output,
        retrieval_query=QUERY,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # Setup
        create_bucket,
        create_execution_role,
        create_opensearch_policies,
        collection,
        await_collection,
        # Step 1 - Create OSS policies and collection
        create_oss_policy_attach_bedrock_execution_role(create_execution_role),
        # Step 2 - Create vector index
        create_vector_index(index_name=index_name, collection_id=collection, region=region_name),
        # Step 3 - Load data to S3 Bucket
        copy_data_to_s3(bucket=bucket_name),
        # Step 4 - Create Knowledge Base
        create_knowledge_base,
        create_data_source,
        # Step 5 - Start ingestion job
        ingest_data,
        # Test the knowledge base using RetrieveAndGenerate API
        knowledge_base_rag,
        # Test the knowledge base using Retrieve API
        retrieve,
        # Clean up
        delete_data_source(
            knowledge_base_id=create_knowledge_base.output,
            data_source_id=create_data_source.output,
        ),
        delete_knowledge_base(knowledge_base_id=create_knowledge_base.output),
        delete_vector_index(index_name=index_name, collection_id=collection),
        delete_opensearch_policies(collection_name=vector_store_name),
        delete_collection(collection_id=collection),
        iam_cleanup(),
        delete_bucket,
    )
