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

import boto3
from flask import request, g

NAMESPACE = "Benchmark"
VERSION = "Iteration3"

client = boto3.client("cloudwatch")

def emit_metrics(func):
    def decorated(*args, **kwargs):
        before = datetime.now()
        ret = func(*args, **kwargs)
        latency = (datetime.now() - before).total_seconds()
        path = request.path
        # input_hash = str(hash(jsonpickle.encode(kwargs)))

        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": "is_authorized_latency",
                    "Dimensions": [
                        {
                            "Name": "path",
                            "Value": path
                        },
                        {
                            "Name": "version",
                            "Value": VERSION
                        },
                    ],
                    "Timestamp": datetime.now(),
                    "Value": latency * 1000,
                    "Unit": "Milliseconds",
                },
            ]
        )
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": "is_authorized_latency",
                    "Dimensions": [
                        {
                            "Name": "version",
                            "Value": VERSION
                        },
                    ],
                    "Timestamp": datetime.now(),
                    "Value": latency * 1000,
                    "Unit": "Milliseconds",
                },
            ]
        )
        return ret
    return decorated
