import click
import subprocess
import os

@click.command()
@click.option('--aws_access_key_id', help="AWS access key ID", required=False)
@click.option('--aws_secret_access_key', help="AWS secret access key", required=False)
@click.option('--aws_default_region', help="AWS default region", required=False)
@click.option('--aws_session_token', help="AWS session token", required=False)
@click.option('--s3_url', help="S3 URL", required=False)
@click.option('--requirements_path', help="Path to requirements file", required=False)
def build_docker_image(
    aws_access_key_id,
    aws_secret_access_key,
    aws_default_region,
    aws_session_token,
    s3_url,
    requirements_path
):
    """Build a Docker image with optional build arguments."""
    build_args = []

    if aws_access_key_id:
        build_args.extend(['--build-arg', f'aws_access_key_id={aws_access_key_id}'])
    
    if aws_secret_access_key:
        build_args.extend(['--build-arg', f'aws_secret_access_key={aws_secret_access_key}'])
    
    if aws_default_region:
        build_args.extend(['--build-arg', f'aws_default_region={aws_default_region}'])
    
    if aws_session_token:
        build_args.extend(['--build-arg', f'aws_session_token={aws_session_token}'])
    
    if s3_url:
        build_args.extend(['--build-arg', f's3_url={s3_url}'])
    
    if requirements_path:
        if not os.path.exists(requirements_path):
            with open(requirements_path, 'w') as f:
                pass
        
        build_args.extend(['--build-arg', f'REQUIREMENTS_PATH={requirements_path}'])

    click.echo(f'Building Docker image with build arguments: {build_args}...')
    
    subprocess.run([
        'docker',
        'build',
        '-t', 'my_custom_image_from_click',
        *build_args,
        '--no-cache',
        '.'
    ])

    click.echo('Docker image built successfully!')

if __name__ == '__main__':
    build_docker_image()
