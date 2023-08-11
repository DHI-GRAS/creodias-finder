import shutil
from pathlib import Path
import concurrent.futures
from multiprocessing.pool import ThreadPool

import requests
from tqdm import tqdm

DOWNLOAD_URL = "https://zipper.creodias.eu/download"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


def _get_token(username, password):
    token_data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    response = requests.post(TOKEN_URL, data=token_data).json()
    try:
        return response["access_token"]
    except KeyError:
        raise RuntimeError(f"Unable to get token. Response was {response}")


def download(uid, username, password, outfile, show_progress=True, token=None):
    """Download a file from CreoDIAS to the given location

    Parameters
    ----------
    uid:
        CreoDIAS UID to download
    username:
        Username
    password:
        Password
    outfile:
        Path where incomplete downloads are stored
    """
    token = token if token else _get_token(username, password)
    url = f"{DOWNLOAD_URL}/{uid}?token={token}"
    _download_raw_data(url, outfile, show_progress)


def download_from_s3(source_path, outdir, s3_client=None):
    """Download a file from CreoDIAS S3 storage to the given location
       (Works only when used from a CreoDIAS vm)

    Parameters
    ----------
    source_path:
        CreoDIAS path to S3 object
    target_path:
        Path to write the product folder
    """
    import boto3
    from botocore.client import Config
    import os

    from creodias_finder.creodias_storage import S3Storage

    if not s3_client:
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://data.cloudferro.com/",
            use_ssl=False,
            aws_access_key_id="access",
            aws_secret_access_key="secret",
            config=Config(
                signature_version="s3",
                connect_timeout=60,
                read_timeout=60,
            ),
        )
    storage_client = S3Storage(s3_client)
    source_path = source_path.lstrip("/eodata/")
    product_folder = source_path.split("/")[-1]
    storage_client.download_product(
        "DIAS", source_path, os.path.join(outdir, product_folder)
    )


def download_list_from_s3(source_paths, outdir, threads=5):
    import boto3
    from botocore.client import Config

    from creodias_finder.creodias_storage import S3Storage

    s3_client = boto3.client(
        "s3",
        endpoint_url="http://data.cloudferro.com/",
        use_ssl=False,
        aws_access_key_id="access",
        aws_secret_access_key="secret",
        config=Config(
            signature_version="s3",
            connect_timeout=60,
            read_timeout=60,
        ),
    )
    pool = ThreadPool(threads)
    download_lambda = lambda x: download_from_s3(x, outdir, s3_client)
    pool.map(download_lambda, source_paths)


def download_list(uids, username, password, outdir, threads=1, show_progress=True):
    """Downloads a list of UIDS

    Parameters
    ----------
    uids:
        A list of UIDs
    username:
        Username
    password:
        Password
    outdir:
        Output direcotry
    threads:
        Number of simultaneous downloads

    Returns
    -------
    dict
        mapping uids to paths to downloaded files
    """
    if show_progress:
        pbar = tqdm(total=len(uids), unit="files")

    token = _get_token(username, password)

    def _download(uid):
        outfile = Path(outdir) / f"{uid}.zip"
        download(
            uid, username, password, outfile=outfile, show_progress=False, token=token
        )
        if show_progress:
            pbar.update(1)
        return uid, outfile

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        paths = dict(executor.map(_download, uids))

    return paths


def _download_raw_data(url, outfile, show_progress):
    """Downloads data from url to outfile.incomplete and then moves to outfile"""
    outfile_temp = str(outfile) + ".incomplete"
    try:
        downloaded_bytes = 0
        with requests.get(url, stream=True, timeout=100) as req:
            print(req.status_code)
            with tqdm(unit="B", unit_scale=True, disable=not show_progress) as progress:
                chunk_size = 2**20  # download in 1 MB chunks
                with open(outfile_temp, "wb") as fout:
                    for chunk in req.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            fout.write(chunk)
                            progress.update(len(chunk))
                            downloaded_bytes += len(chunk)
        shutil.move(outfile_temp, outfile)
    finally:
        try:
            Path(outfile_temp).unlink()
        except OSError:
            pass
