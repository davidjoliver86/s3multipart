"""
s3multipart is a simple wrapper around AWS S3 multi-part uploads.
==
This script assumes you've already split your file into multiple parts, and that those parts
have numerically sequential extensions (e.g. .01, .02, ...). Leading zeroes are acceptable but
must be consistent.

This script also assumes AES-256 server-side encoding. Encryption is good.

Usage:
  s3multipart init <BUCKET> <KEY>
    - Starts a new multipart upload. The response metadata is saved as multipart.json.
  s3multipart upload <SOURCE_FOLDER>
    - Uploads the parts of each file. Saves each parts' ETag in multipart.json for finalization.
  s3multipart abort
    - Aborts the multipart upload.
  s3multipart finalize
    - Finalizes the multipart upload and creates a new S3 object.
"""
import re
import os
import sys
import json
import pathlib
import functools

import click
import boto3

MULTIPART_FILENAME = pathlib.Path('multipart.json')


def _s3_session():
    return boto3.session.Session().client('s3')


def error(msg):
    """
    Echoes an error message and immediately exits.
    """
    click.secho(msg, fg='red')
    sys.exit(1)


@click.group()
def cli():
    """
    Base command group.
    """


def check_multipart(func):
    """
    Wrapper that checks for the existence of a multipart upload and errors out if there isn't one.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not MULTIPART_FILENAME.exists():
            error('No active multipart upload in progress!')
        func(*args, **kwargs)
    return wrapper


@cli.command()
@click.argument('bucket')
@click.argument('key')
def init(bucket: str, key: str) -> None:
    """
    Initializes a new multipart upload.
    """
    session = _s3_session()
    mpupload = session.create_multipart_upload(Bucket=bucket, Key=key, ServerSideEncryption='AES256')
    with MULTIPART_FILENAME.open('w') as fp:
        fp.write(json.dumps(mpupload))
    click.secho(f'Started multipart upload for s3://{bucket}/{key}', fg='green')


@cli.command()
@click.argument('src')
@check_multipart
def upload(src: str) -> None:
    """
    Uploads all 'part' files in 'src'. The PartNumber is derived from the extensions, which should be
    numerically sequential.
    """
    with MULTIPART_FILENAME.open('r') as fp:
        data = json.load(fp)
        bucket = data['Bucket']
        key = data['Key']
        upload_id = data['UploadId']

    # find file chunks (a folder ending in .1, .2, .3... leading zeros okay)
    if not pathlib.Path(src).is_dir():
        error('Please pass in a folder containing the file parts')
    parts = sorted([f for f in pathlib.Path(src).iterdir() if re.match(r'\.[0-9]+', f.suffix)])
    if not parts:
        error(f'Unable to find file parts in {src}!')
    click.secho(f'Found the following file parts in {src}:', fg='green')
    for part in parts:
        click.secho(part.name, fg='blue')
    proceed = click.confirm('Proceed?')
    if not proceed:
        sys.exit(0)

    # do the uploads
    session = _s3_session()
    with click.progressbar(parts, item_show_func=str) as progress_bar:
        for part in progress_bar:
            part_number = int(part.suffix[1:])
            with part.open('rb') as fp:
                resp = session.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part_number, Body=fp)
                etag = resp['ETag'][1:-1]  # strips extra quote characters
            with MULTIPART_FILENAME.open('r') as fp:
                data = json.load(fp)
            data['Parts'] = data.get('Parts', []) + [{'ETag': etag, 'PartNumber': part_number}]
            with MULTIPART_FILENAME.open('w') as fp:
                json.dump(data, fp)


@cli.command()
@check_multipart
def abort() -> None:
    """
    Aborts the multipart upload.
    """
    session = _s3_session()
    with MULTIPART_FILENAME.open('r') as fp:
        data = json.load(fp)
        bucket = data['Bucket']
        key = data['Key']
        upload_id = data['UploadId']
        resp = session.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    if resp['ResponseMetadata']['HTTPStatusCode'] == 204:
        os.remove(MULTIPART_FILENAME)
        click.secho(f'Aborted multipart upload for s3://{bucket}/{key}', fg='yellow')
    else:
        click.secho('Bad HTTP response', fg='red')
        click.secho(json.dumps(resp, indent=2), fg='red')

@cli.command()
@check_multipart
def finalize() -> None:
    """
    Finalizes the multipart upload and prints out the S3 key.
    """
    session = _s3_session()
    with MULTIPART_FILENAME.open('r') as fp:
        data = json.load(fp)
        bucket = data['Bucket']
        key = data['Key']
        upload_id = data['UploadId']
        mpu = {'Parts': data['Parts']}
    resp = session.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload=mpu)
    if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        os.remove(MULTIPART_FILENAME)
        click.secho(f'Finalized multipart upload for s3://{bucket}/{key}', fg='green')
    else:
        click.secho('Bad HTTP response', fg='red')
        click.secho(json.dumps(resp, indent=2), fg='red')

def main():
    """
    Script entrypoint.
    """
    cli()
