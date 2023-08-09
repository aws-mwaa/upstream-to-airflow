# Dockerfile for Apache Airflow with AWS CLI Integration

This Dockerfile creates an image that can be used on an ECS container to run tasks using the AWS ECS Executor in Apache Airflow. The image
supports AWS CLI integration, allowing you to interact with AWS services within your Airflow environment. It also includes options to load DAGs (Directed Acyclic Graphs) from either an S3 bucket or a local folder.

## Base Image
The Docker image is built upon the `apache/airflow:latest` image. See [here](https://hub.docker.com/r/apache/airflow) for more infomation about the image.
Important note: The python version in the base image must match the python version of the Airflow instance associated with the ECS Executor.

## Prerequisites
Docker must be installed on your system. Instructions for installing Docker can be found [here](https://docs.docker.com/get-docker/).

## AWS CLI Configuration
The [AWS CLI](https://docs.docker.com/get-docker/) is installed within the container, and there are multiple ways to pass AWS authentication information to the container. This guide will cover 2 methods.

The first method is to use the build-time arguments (`aws_access_key_id`, `aws_secret_access_key`, `aws_default_region`, and `aws_session_token`).
To pass AWS authentication information using these arguments, use the `--build-arg` option during the Docker build process. For example:

```
docker build -t my-airflow-image \
 --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
 --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
 --build-arg aws_default_region=YOUR_DEFAULT_REGION \
 --build-arg aws_session_token=YOUR_SESSION_TOKEN .
```

Replace `YOUR_ACCESS_KEY`, `YOUR_SECRET_KEY`, `YOUR_SESSION_TOKEN`, and `YOUR_DEFAULT_REGION` with valid AWS credentials.

Alternatively, you can authenticate to AWS using the `~/.aws` folder. See instructions on how to generate this folder [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Uncomment the line in the Dockerfile to copy the `./.aws` folder from your host machine to the container's `/home/airflow/.aws` directory. Keep in mind the Docker build context when copying the `.aws` folder to the container.
## Loading DAGs
There are many ways to load DAGs on the ECS container. This Dockerfile is preconfigured with two possible ways: copying from a local folder, or downloading from an S3 bucket. Other methods of loading DAGs are possible as well.

### From S3 Bucket
To load DAGs from an S3 bucket, uncomment the line in the Dockerfile to synchronize the DAGs from the specified S3 bucket to the `/opt/airflow/dags` directory inside the container. You can optionally provide `container_dag_path` as a build argument if you want to store the DAGs in a directory other than `/opt/airflow/dags`.

Add `--build-arg s3_url=YOUR_S3_URL` in the docker build command.
Replace `YOUR_S3_URL` with the URL of your S3 bucket. Make sure you have the appropriate permissions to read from the bucket. 

Note that the following command is also passing in AWS credentials as build arguments.
```
docker build -t my-airflow-image \
 --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
 --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
 --build-arg aws_default_region=YOUR_DEFAULT_REGION \
 --build-arg aws_session_token=YOUR_SESSION_TOKEN \
 --build-arg s3_url=YOUR_S3_URL -t my-airflow-image .
```


### From Local Folder
To load DAGs from a local folder, place your DAG files in a folder within the docker build context on your host machine, and provide the location of the folder using the `host_dag_path` build argument. By default, the DAGs will be copied to `/opt/airflow/dags`, but this can be changed by passing the `container_dag_path` build-time argument during the Docker build process:

```
docker build -t my-airflow-image --build-arg host_dag_path=./dags_on_host --build-arg container_dag_path=/path/on/container .
```
If choosing to load DAGs onto a different path than `/opt/airflow/dags`, then the new path will need to be updated in the Airflow config. 
### Mounting a Volume
You can optionally mount a local directory as a volume on the container during run-time. This will allow you to make change to files on the mounted directory, and have those changes be reflected in the container. To do this, run the following command:
```
docker run --volume /abs/path/to/local/dir:/abs/path/to/remote/dir <image_name>
```
Note: Doing this will overwrite the contents of the directory on the container with the contents of the local directory.

### Building Image for ECS Executor
Detailed instructions on how to use the Docker image, that you have created via this readme, with the ECS Executor can be found [here](link_to_how_to_guide).
