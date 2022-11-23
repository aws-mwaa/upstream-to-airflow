#
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

import time
from datetime import datetime
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal
from moto import mock_sagemaker

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sagemaker import (
    LogState,
    SageMakerHook,
    secondary_training_status_changed,
    secondary_training_status_message,
)

ROLE = "arn:aws:iam:role/test-role"

PATH = "local/data"
BUCKET = "test-bucket"
KEY = "test/data"
DATA_URL = f"s3://{BUCKET}/{KEY}"

JOB_NAME = "test-job"
MODEL_NAME = "test-model"
CONFIG_NAME = "test-endpoint-config"
ENDPOINT_NAME = "test-endpoint"

IMAGE = "test-image"
TEST_ARN_RETURN = {"Arn": "testarn"}
OUTPUT_URL = f"s3://{BUCKET}/test/output"

IN_PROGRESS = "InProgress"

CREATE_TRAINING_PARAMS = {
    "AlgorithmSpecification": {"TrainingImage": IMAGE, "TrainingInputMode": "File"},
    "RoleArn": ROLE,
    "OutputDataConfig": {"S3OutputPath": OUTPUT_URL},
    "ResourceConfig": {"InstanceCount": 2, "InstanceType": "ml.c4.8xlarge", "VolumeSizeInGB": 50},
    "TrainingJobName": JOB_NAME,
    "HyperParameters": {"k": "10", "feature_dim": "784", "mini_batch_size": "500", "force_dense": "True"},
    "StoppingCondition": {"MaxRuntimeInSeconds": 60 * 60},
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": DATA_URL,
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None",
        },
        {
            "ChannelName": "train_fs",
            "DataSource": {
                "FileSystemDataSource": {
                    "DirectoryPath": "/tmp",
                    "FileSystemAccessMode": "ro",
                    "FileSystemId": "fs-abc",
                    "FileSystemType": "FSxLustre",
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None",
        },
    ],
}

CREATE_TUNING_PARAMS = {
    "HyperParameterTuningJobName": JOB_NAME,
    "HyperParameterTuningJobConfig": {
        "Strategy": "Bayesian",
        "HyperParameterTuningJobObjective": {"Type": "Maximize", "MetricName": "test_metric"},
        "ResourceLimits": {"MaxNumberOfTrainingJobs": 123, "MaxParallelTrainingJobs": 123},
        "ParameterRanges": {
            "IntegerParameterRanges": [
                {"Name": "k", "MinValue": "2", "MaxValue": "10"},
            ]
        },
    },
    "TrainingJobDefinition": {
        "StaticHyperParameters": CREATE_TRAINING_PARAMS["HyperParameters"],
        "AlgorithmSpecification": CREATE_TRAINING_PARAMS["AlgorithmSpecification"],
        "RoleArn": "string",
        "InputDataConfig": CREATE_TRAINING_PARAMS["InputDataConfig"],
        "OutputDataConfig": CREATE_TRAINING_PARAMS["OutputDataConfig"],
        "ResourceConfig": CREATE_TRAINING_PARAMS["ResourceConfig"],
        "StoppingCondition": dict(MaxRuntimeInSeconds=60 * 60),
    },
}

CREATE_TRANSFORM_PARAMS = {
    "TransformJobName": JOB_NAME,
    "ModelName": MODEL_NAME,
    "BatchStrategy": "MultiRecord",
    "TransformInput": {"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": DATA_URL}}},
    "TransformOutput": {
        "S3OutputPath": OUTPUT_URL,
    },
    "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": 123},
}

CREATE_TRANSFORM_PARAMS_FS = {
    "TransformJobName": JOB_NAME,
    "ModelName": MODEL_NAME,
    "BatchStrategy": "MultiRecord",
    "TransformInput": {
        "DataSource": {
            "FileSystemDataSource": {
                "DirectoryPath": "/tmp",
                "FileSystemAccessMode": "ro",
                "FileSystemId": "fs-abc",
                "FileSystemType": "FSxLustre",
            }
        }
    },
    "TransformOutput": {
        "S3OutputPath": OUTPUT_URL,
    },
    "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": 123},
}

CREATE_MODEL_PARAMS = {
    "ModelName": MODEL_NAME,
    "PrimaryContainer": {
        "Image": IMAGE,
        "ModelDataUrl": OUTPUT_URL,
    },
    "ExecutionRoleArn": ROLE,
}

CREATE_ENDPOINT_CONFIG_PARAMS = {
    "EndpointConfigName": CONFIG_NAME,
    "ProductionVariants": [
        {
            "VariantName": "AllTraffic",
            "ModelName": MODEL_NAME,
            "InitialInstanceCount": 1,
            "InstanceType": "ml.c4.xlarge",
        }
    ],
}

CREATE_ENDPOINT_PARAMS = {"EndpointName": ENDPOINT_NAME, "EndpointConfigName": CONFIG_NAME}

UPDATE_ENDPOINT_PARAMS = CREATE_ENDPOINT_PARAMS

DESCRIBE_TRAINING_COMPLETED_RETURN = {
    "TrainingJobStatus": "Completed",
    "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.c4.xlarge", "VolumeSizeInGB": 10},
    "TrainingStartTime": datetime(2018, 2, 17, 7, 15, 0, 103000),
    "TrainingEndTime": datetime(2018, 2, 17, 7, 19, 34, 953000),
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}

DESCRIBE_TRAINING_INPROGRESS_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_INPROGRESS_RETURN.update({"TrainingJobStatus": IN_PROGRESS})

DESCRIBE_TRAINING_FAILED_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_FAILED_RETURN.update({"TrainingJobStatus": "Failed", "FailureReason": "Unknown"})

DESCRIBE_TRAINING_STOPPING_RETURN = dict(DESCRIBE_TRAINING_COMPLETED_RETURN)
DESCRIBE_TRAINING_STOPPING_RETURN.update({"TrainingJobStatus": "Stopping"})

MESSAGE = "message"
STATUS = "status"
SECONDARY_STATUS_DESCRIPTION_1 = {
    "SecondaryStatusTransitions": [{"StatusMessage": MESSAGE, "Status": STATUS}]
}
SECONDARY_STATUS_DESCRIPTION_2 = {
    "SecondaryStatusTransitions": [{"StatusMessage": "different message", "Status": STATUS}]
}

DEFAULT_LOG_STREAMS = {"logStreams": [{"logStreamName": JOB_NAME + "/xxxxxxxxx"}]}
LIFECYCLE_LOG_STREAMS = [
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
]

DEFAULT_LOG_EVENTS = [
    {"nextForwardToken": None, "events": [{"timestamp": 1, "message": "hi there #1"}]},
    {"nextForwardToken": None, "events": []},
]
STREAM_LOG_EVENTS = [
    {"nextForwardToken": None, "events": [{"timestamp": 1, "message": "hi there #1"}]},
    {"nextForwardToken": None, "events": []},
    {
        "nextForwardToken": None,
        "events": [{"timestamp": 1, "message": "hi there #1"}, {"timestamp": 2, "message": "hi there #2"}],
    },
    {"nextForwardToken": None, "events": []},
    {
        "nextForwardToken": None,
        "events": [
            {"timestamp": 2, "message": "hi there #2"},
            {"timestamp": 2, "message": "hi there #2a"},
            {"timestamp": 3, "message": "hi there #3"},
        ],
    },
    {"nextForwardToken": None, "events": []},
]

TEST_EVALUATION_CONFIG = {
    "Image": IMAGE,
    "Role": ROLE,
    "S3Operations": {
        "S3CreateBucket": [{"Bucket": BUCKET}],
        "S3Upload": [{"Path": PATH, "Bucket": BUCKET, "Key": KEY, "Tar": False}],
    },
}


@pytest.fixture
def mock_conn():
    with mock.patch.object(SageMakerHook, "get_conn") as _get_conn:
        yield _get_conn.return_value


class TestSageMakerHook:
    @mock.patch.object(AwsLogsHook, "get_log_events")
    def test_multi_stream_iter(self, mock_log_stream):
        event = {"timestamp": 1}
        mock_log_stream.side_effect = [iter([event]), iter([]), None]

        event_iter = SageMakerHook().multi_stream_iter("log", [None, None, None])

        assert next(event_iter) == (0, event)

    @mock.patch.object(S3Hook, "create_bucket")
    @mock.patch.object(S3Hook, "load_file")
    def test_configure_s3_resources(self, mock_load_file, mock_create_bucket):
        evaluation_result = {"Image": IMAGE, "Role": ROLE}

        SageMakerHook().configure_s3_resources(TEST_EVALUATION_CONFIG)

        assert TEST_EVALUATION_CONFIG == evaluation_result
        mock_create_bucket.assert_called_once_with(bucket_name=BUCKET)
        mock_load_file.assert_called_once_with(PATH, KEY, BUCKET)

    @mock.patch.object(S3Hook, "check_for_key", side_effect=[False, True, False])
    @mock.patch.object(S3Hook, "check_for_bucket", side_effect=[False, True, True, True])
    @mock.patch.object(S3Hook, "check_for_prefix", side_effect=[False, True, True])
    def test_check_s3_url(self, *_):
        hook = SageMakerHook()

        with pytest.raises(AirflowException):
            hook.check_s3_url(DATA_URL)
        with pytest.raises(AirflowException):
            hook.check_s3_url(DATA_URL)

        assert hook.check_s3_url(DATA_URL) is True
        assert hook.check_s3_url(DATA_URL) is True

    @mock.patch.object(SageMakerHook, "check_s3_url")
    def test_check_valid_training(self, mock_check_url):
        SageMakerHook().check_training_config(CREATE_TRAINING_PARAMS)

        mock_check_url.assert_called_once_with(DATA_URL)

        # InputDataConfig is optional, verify if check succeeds without InputDataConfig
        create_training_params_no_inputdataconfig = CREATE_TRAINING_PARAMS.copy()
        create_training_params_no_inputdataconfig.pop("InputDataConfig")
        SageMakerHook().check_training_config(create_training_params_no_inputdataconfig)

    @mock.patch.object(SageMakerHook, "check_s3_url")
    def test_check_valid_tuning(self, mock_check_url):
        SageMakerHook().check_tuning_config(CREATE_TUNING_PARAMS)

        mock_check_url.assert_called_once_with(DATA_URL)

    def test_conn(self):
        conn_id = "sagemaker_test_conn_id"

        hook = SageMakerHook(aws_conn_id=conn_id)

        assert hook.aws_conn_id == conn_id

    @mock.patch.object(SageMakerHook, "check_training_config")
    def test_create_training_job(self, _, mock_conn):
        mock_conn.create_training_job.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_training_job(
            CREATE_TRAINING_PARAMS, wait_for_completion=False, print_log=False
        )

        mock_conn.create_training_job.assert_called_once_with(**CREATE_TRAINING_PARAMS)
        assert response == TEST_ARN_RETURN

    @mock.patch.object(SageMakerHook, "check_training_config")
    @mock.patch("time.sleep", return_value=None)
    def test_training_ends_with_wait(self, mock_sleep, _, mock_conn):
        mock_conn.create_training_job.return_value = TEST_ARN_RETURN
        mock_conn.describe_training_job.side_effect = [
            DESCRIBE_TRAINING_INPROGRESS_RETURN,
            DESCRIBE_TRAINING_STOPPING_RETURN,
            DESCRIBE_TRAINING_COMPLETED_RETURN,
        ]

        SageMakerHook().create_training_job(
            CREATE_TRAINING_PARAMS, wait_for_completion=True, print_log=False, check_interval=0
        )

        assert mock_conn.describe_training_job.call_count == 3

    @mock.patch.object(SageMakerHook, "check_training_config")
    @mock.patch("time.sleep", return_value=None)
    def test_training_throws_error_when_failed_with_wait(self, mock_sleep, _, mock_conn):
        mock_conn.create_training_job.return_value = TEST_ARN_RETURN
        mock_conn.describe_training_job.side_effect = [
            DESCRIBE_TRAINING_INPROGRESS_RETURN,
            DESCRIBE_TRAINING_STOPPING_RETURN,
            DESCRIBE_TRAINING_FAILED_RETURN,
            DESCRIBE_TRAINING_COMPLETED_RETURN,
        ]

        with pytest.raises(AirflowException):
            SageMakerHook().create_training_job(
                CREATE_TRAINING_PARAMS,
                wait_for_completion=True,
                print_log=False,
                check_interval=0,
            )

        assert mock_conn.describe_training_job.call_count == 3

    @mock.patch.object(SageMakerHook, "check_tuning_config")
    def test_create_tuning_job(self, _, mock_conn):
        mock_conn.create_hyper_parameter_tuning_job.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_tuning_job(CREATE_TUNING_PARAMS, wait_for_completion=False)

        mock_conn.create_hyper_parameter_tuning_job.assert_called_once_with(**CREATE_TUNING_PARAMS)
        assert response == TEST_ARN_RETURN

    @mock.patch.object(SageMakerHook, "check_s3_url")
    def test_create_transform_job(self, mock_check_url, mock_conn):
        mock_check_url.return_value = True
        mock_conn.create_transform_job.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_transform_job(CREATE_TRANSFORM_PARAMS, wait_for_completion=False)

        mock_conn.create_transform_job.assert_called_once_with(**CREATE_TRANSFORM_PARAMS)
        assert response == TEST_ARN_RETURN

    def test_create_transform_job_fs(self, mock_conn):
        mock_conn.create_transform_job.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_transform_job(CREATE_TRANSFORM_PARAMS_FS, wait_for_completion=False)

        mock_conn.create_transform_job.assert_called_once_with(**CREATE_TRANSFORM_PARAMS_FS)
        assert response == TEST_ARN_RETURN

    def test_create_model(self, mock_conn):
        mock_conn.create_model.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_model(CREATE_MODEL_PARAMS)

        mock_conn.create_model.assert_called_once_with(**CREATE_MODEL_PARAMS)
        assert response == TEST_ARN_RETURN

    def test_create_endpoint_config(self, mock_conn):
        mock_conn.create_endpoint_config.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_endpoint_config(CREATE_ENDPOINT_CONFIG_PARAMS)

        mock_conn.create_endpoint_config.assert_called_once_with(**CREATE_ENDPOINT_CONFIG_PARAMS)
        assert response == TEST_ARN_RETURN

    def test_create_endpoint(self, mock_conn):
        mock_conn.create_endpoint.return_value = TEST_ARN_RETURN

        response = SageMakerHook().create_endpoint(CREATE_ENDPOINT_PARAMS, wait_for_completion=False)

        mock_conn.create_endpoint.assert_called_once_with(**CREATE_ENDPOINT_PARAMS)
        assert response == TEST_ARN_RETURN

    def test_update_endpoint(self, mock_conn):
        mock_conn.update_endpoint.return_value = TEST_ARN_RETURN

        response = SageMakerHook().update_endpoint(UPDATE_ENDPOINT_PARAMS, wait_for_completion=False)

        mock_conn.update_endpoint.assert_called_once_with(**UPDATE_ENDPOINT_PARAMS)
        assert response == TEST_ARN_RETURN

    def test_describe_training_job(self, mock_conn):
        mock_conn.describe_training_job.return_value = IN_PROGRESS

        response = SageMakerHook().describe_training_job(JOB_NAME)

        mock_conn.describe_training_job.assert_called_once_with(TrainingJobName=JOB_NAME)
        assert response == IN_PROGRESS

    def test_describe_tuning_job(self, mock_conn):
        mock_conn.describe_hyper_parameter_tuning_job.return_value = IN_PROGRESS

        response = SageMakerHook().describe_tuning_job(JOB_NAME)

        mock_conn.describe_hyper_parameter_tuning_job.assert_called_once_with(
            HyperParameterTuningJobName=JOB_NAME
        )
        assert response == IN_PROGRESS

    def test_describe_transform_job(self, mock_conn):
        mock_conn.describe_transform_job.return_value = IN_PROGRESS

        response = SageMakerHook().describe_transform_job(JOB_NAME)

        mock_conn.describe_transform_job.assert_called_once_with(TransformJobName=JOB_NAME)
        assert response == IN_PROGRESS

    def test_describe_model(self, mock_conn):
        mock_conn.describe_model.return_value = MODEL_NAME

        response = SageMakerHook().describe_model(MODEL_NAME)

        mock_conn.describe_model.assert_called_once_with(ModelName=MODEL_NAME)
        assert response == MODEL_NAME

    def test_describe_endpoint_config(self, mock_conn):
        mock_conn.describe_endpoint_config.return_value = CONFIG_NAME

        response = SageMakerHook().describe_endpoint_config(CONFIG_NAME)

        mock_conn.describe_endpoint_config.assert_called_once_with(EndpointConfigName=CONFIG_NAME)
        assert response == CONFIG_NAME

    def test_describe_endpoint(self, mock_conn):
        mock_conn.describe_endpoint.return_value = IN_PROGRESS

        response = SageMakerHook().describe_endpoint(ENDPOINT_NAME)

        mock_conn.describe_endpoint.assert_called_once_with(EndpointName=ENDPOINT_NAME)
        assert response == IN_PROGRESS

    def test_secondary_training_status_changed_true(self):
        changed = secondary_training_status_changed(
            SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_2
        )
        assert changed

    def test_secondary_training_status_changed_false(self):
        changed = secondary_training_status_changed(
            SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_1
        )
        assert not changed

    def test_secondary_training_status_message_status_changed(self):
        now = datetime.now(tzlocal())
        SECONDARY_STATUS_DESCRIPTION_1["LastModifiedTime"] = now
        expected_time = datetime.utcfromtimestamp(time.mktime(now.timetuple())).strftime("%Y-%m-%d %H:%M:%S")
        expected = f"{expected_time} {STATUS} - {MESSAGE}"
        assert (
            secondary_training_status_message(SECONDARY_STATUS_DESCRIPTION_1, SECONDARY_STATUS_DESCRIPTION_2)
            == expected
        )

    @mock.patch.object(AwsLogsHook, "get_conn")
    @mock.patch.object(time, "monotonic")
    def test_describe_training_job_with_logs_in_progress(self, mock_time, mock_log_conn, mock_conn):
        mock_time.return_value = 50
        mock_conn.describe_training_job.return_value = DESCRIBE_TRAINING_COMPLETED_RETURN
        mock_log_conn.return_value.describe_log_streams.side_effect = LIFECYCLE_LOG_STREAMS
        mock_log_conn.return_value.get_log_events.side_effect = STREAM_LOG_EVENTS

        response = SageMakerHook().describe_training_job_with_log(
            job_name=JOB_NAME,
            positions={},
            stream_names=[],
            instance_count=1,
            state=LogState.WAIT_IN_PROGRESS,
            last_description={},
            last_describe_job_call=0,
        )

        assert response == (LogState.JOB_COMPLETE, {}, 50)

    @pytest.mark.parametrize("log_state", [LogState.JOB_COMPLETE, LogState.COMPLETE])
    @mock.patch.object(AwsLogsHook, "get_conn")
    def test_describe_training_job_with_complete_states(self, mock_log_conn, log_state, mock_conn):
        mock_conn.describe_training_job.return_value = DESCRIBE_TRAINING_COMPLETED_RETURN
        mock_log_conn.return_value.describe_log_streams.side_effect = LIFECYCLE_LOG_STREAMS
        mock_log_conn.return_value.get_log_events.side_effect = STREAM_LOG_EVENTS

        response = SageMakerHook().describe_training_job_with_log(
            job_name=JOB_NAME,
            positions={},
            stream_names=[],
            instance_count=1,
            state=log_state,
            last_description={},
            last_describe_job_call=0,
        )

        assert response == (LogState.COMPLETE, {}, 0)

    @mock.patch.object(SageMakerHook, "check_training_config", return_value=True)
    @mock.patch("time.sleep", return_value=None)
    @mock.patch.object(AwsLogsHook, "get_conn")
    @mock.patch.object(SageMakerHook, "describe_training_job_with_log")
    def test_training_with_logs(self, mock_describe, mock_log_conn, mock_sleep, _, mock_conn):
        mock_describe.side_effect = [
            (LogState.WAIT_IN_PROGRESS, DESCRIBE_TRAINING_INPROGRESS_RETURN, 0),
            (LogState.JOB_COMPLETE, DESCRIBE_TRAINING_STOPPING_RETURN, 0),
            (LogState.COMPLETE, DESCRIBE_TRAINING_COMPLETED_RETURN, 0),
        ]
        mock_conn.create_training_job.return_value = TEST_ARN_RETURN
        mock_conn.describe_training_job.return_value = DESCRIBE_TRAINING_COMPLETED_RETURN
        mock_log_conn.return_value.describe_log_streams.side_effect = LIFECYCLE_LOG_STREAMS
        mock_log_conn.return_value.get_log_events.side_effect = STREAM_LOG_EVENTS

        SageMakerHook().create_training_job(
            CREATE_TRAINING_PARAMS, wait_for_completion=True, print_log=True, check_interval=0
        )

        assert mock_describe.call_count == 3
        assert mock_conn.describe_training_job.call_count == 1

    def test_find_processing_job_by_name(self, mock_conn):
        mock_conn.list_processing_jobs.return_value = {
            "ProcessingJobSummaries": [{"ProcessingJobName": "existing_job"}]
        }

        with pytest.warns(DeprecationWarning):
            result = SageMakerHook().find_processing_job_by_name("existing_job")
            assert result

    def test_find_processing_job_by_name_job_not_exists_should_return_false(self, mock_conn):
        error_resp = {"Error": {"Code": "ValidationException"}}
        mock_conn.describe_processing_job.side_effect = ClientError(
            error_response=error_resp, operation_name="empty"
        )

        with pytest.warns(DeprecationWarning):
            result = SageMakerHook().find_processing_job_by_name("existing_job")
            assert not result

    def test_count_processing_jobs_by_name(self, mock_conn):
        existing_job_name = "existing_job"
        mock_conn.list_processing_jobs.return_value = {
            "ProcessingJobSummaries": [{"ProcessingJobName": existing_job_name}]
        }

        result = SageMakerHook().count_processing_jobs_by_name(existing_job_name)

        assert result == 1

    def test_count_processing_jobs_by_name_only_counts_actual_hits(self, mock_conn):
        existing_job_name = "existing_job"
        mock_conn.list_processing_jobs.return_value = {
            "ProcessingJobSummaries": [
                {"ProcessingJobName": existing_job_name},
                {"ProcessingJobName": f"contains_but_does_not_start_with_{existing_job_name}"},
                {"ProcessingJobName": f"{existing_job_name}_with_different_suffix-123"},
            ]
        }
        ret = SageMakerHook().count_processing_jobs_by_name(existing_job_name)
        assert ret == 1

    @mock.patch("time.sleep", return_value=None)
    def test_count_processing_jobs_by_name_retries_on_throttle_exception(self, _, mock_conn):
        throttle_exception = ClientError(
            error_response={"Error": {"Code": "ThrottlingException"}}, operation_name="empty"
        )
        successful_result = {"ProcessingJobSummaries": [{"ProcessingJobName": "existing_job"}]}
        # Return a ThrottleException on the first call, then a mocked successful value the second.
        mock_conn.list_processing_jobs.side_effect = [throttle_exception, successful_result]

        result = SageMakerHook().count_processing_jobs_by_name("existing_job")

        assert mock_conn.list_processing_jobs.call_count == 2
        assert result == 1

    @mock.patch("time.sleep", return_value=None)
    def test_count_processing_jobs_by_name_fails_after_max_retries(self, _, mock_conn):
        mock_conn.list_processing_jobs.side_effect = ClientError(
            error_response={"Error": {"Code": "ThrottlingException"}}, operation_name="empty"
        )
        retry_count = 3

        with pytest.raises(ClientError) as raised_exception:
            SageMakerHook().count_processing_jobs_by_name("existing_job", retries=retry_count)

        assert mock_conn.list_processing_jobs.call_count == retry_count + 1
        assert raised_exception.value.response["Error"]["Code"] == "ThrottlingException"

    def test_count_processing_jobs_by_name_job_not_exists_should_return_falsy(self, mock_conn):
        error_resp = {"Error": {"Code": "ResourceNotFound"}}
        mock_conn.list_processing_jobs.side_effect = ClientError(
            error_response=error_resp, operation_name="empty"
        )

        result = SageMakerHook().count_processing_jobs_by_name("existing_job")

        assert result == 0

    def test_delete_model(self, mock_conn):
        SageMakerHook().delete_model(model_name="test")

        mock_conn.delete_model.assert_called_once_with(ModelName="test")

    @mock_sagemaker
    def test_delete_model_when_not_exist(self):
        with pytest.raises(ClientError) as raised_exception:
            SageMakerHook().delete_model(model_name="test")
        ex = raised_exception.value

        assert ex.operation_name == "DeleteModel"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404
