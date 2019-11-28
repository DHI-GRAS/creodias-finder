''' S3 Storage client.
'''
import os
import errno
import pathlib
from pathlib import Path
from typing import Any, Dict, List, Union

import boto3.session
from boto3.s3.transfer import TransferConfig


class S3Storage:
    ''' S3 product client.
    '''

    def __init__(self, s3_client: boto3.session.Session.client) -> None:
        megabyte = 1024 ** 2
        gigabyte = 1024 ** 3
        self.s3_client = s3_client
        self.s3_config = TransferConfig(io_chunksize=4*megabyte,
                                        multipart_threshold=4*gigabyte)

    def find(self, bucket: str, prefix: str) -> List[Dict[str, Any]]:
        '''List s3 objects in bucket with prefix.

        Args:
            bucket (str): bucket
            prefix (str): prefix

        Raises:
            ValueError: If prefix not in object key.

        Returns:
            List[Dict[str, Any]]: list of objects as dicts
        '''

        objects_list: List[Dict[str, str]] = []
        is_truncated = True
        cont_token = ''
        while is_truncated:
            if not cont_token:
                resp = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=1000)
            else:
                resp = self.s3_client.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, MaxKeys=10000, ContinuationToken=cont_token)
            if 'Contents' not in resp or not resp['Contents']:
                return []
            objects_list.extend(resp['Contents'])
            is_truncated = resp['IsTruncated']
            if is_truncated:
                cont_token = resp['NextContinuationToken']
        return objects_list

    def download_product(self, bucket: str,
                         product_key: Union[pathlib.Path, str],
                         destination: Union[pathlib.Path, str]) -> None:
        '''Download all files beginning with product_key into directory 'destination'.
        Downloaded files will have their prefixes stripped.
        If destination does not exist function will try to make directories.

        Args:
            bucket (str): bucket
            product_key (Union[pathlib.Path, str]): product key - path
            destination (Union[pathlib.Path, str]): directory to put the product
        '''
        if isinstance(destination, Path):
            dest = Path(destination)
        else:
            dest = Path(destination.rstrip('/'))
        pkey = Path(product_key)
        try:
            dest.mkdir(exist_ok=True)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        if isinstance(product_key, Path):
            prefix = str(product_key) + '/'
        else:
            prefix = product_key

        objects_list = self.find(bucket, prefix)
        files: List[Path] = []
        for s3_key in objects_list:
            file_path = s3_key['Key'].replace(product_key, '', 1)
            if file_path:
                if len(file_path.split('/')) > 2:
                    file_folder = os.path.dirname(file_path)
                    dest.joinpath(Path(file_folder.lstrip('/'))).mkdir(
                        parents=True, exist_ok=True)
                    files.append(Path(file_path.lstrip('/')))
                else:
                    files.append(Path(file_path.lstrip('/')))

        for item in files:
            dest.joinpath(item).parent.mkdir(parents=True, exist_ok=True)
            print(pkey)
            print(item)
            print(str(pkey.joinpath(item)))
            #self.s3_client.download_file(bucket, str(
            #    pkey.joinpath(item)), str(dest.joinpath(item)))
