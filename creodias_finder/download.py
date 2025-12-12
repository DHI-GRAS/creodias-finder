import concurrent.futures
import shutil
from multiprocessing.pool import ThreadPool
from pathlib import Path

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


def download(prod, username, password, outfile, show_progress=True, token=None):
    """Download a file from CreoDIAS to the given location

    Parameters
    ----------
    prod:
        CreoDIAS product to download
    username:
        Username
    password:
        Password
    outfile:
        Path where incomplete downloads are stored
    """
    token = token if token else _get_token(username, password)
    uid = prod.get("Id")
    url = f"{DOWNLOAD_URL}/{uid}?token={token}"
    _download_raw_data(url, outfile, show_progress)


def download_from_s3(prod, outdir, s3_client=None, file_filter=""):
    """Download a file from CreoDIAS S3 storage to the given location
       (Works only when used from a CreoDIAS vm)

    Parameters
    ----------
    prod:
        CreoDIAS odata product
    target_path:
        Path to write the product folder
    s3_client:
        S3 client, if None the default one is used
    file_filter:
        Regex expression to filter which product files to download
    """
    import os

    import boto3
    from botocore.client import Config

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
    source_path = prod.get("S3Path")
    source_path = source_path.removeprefix("/eodata/")
    product_folder = source_path.split("/")[-1]
    download_path = os.path.join(outdir, product_folder)
    storage_client.download_product("eodata", source_path, download_path, file_filter)

    return Path(download_path)


def download_list_from_s3(products, outdir, threads=5):
    from functools import partial

    import boto3
    from botocore.client import Config

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
    download_lambda = partial(download_from_s3, outdir=outdir, s3_client=s3_client)
    pool.map(download_lambda, products)


def download_list(products, username, password, outdir, threads=1, show_progress=True):
    """Downloads a list of products

    Parameters
    ----------
    products:
        A list of odata creodias products
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
        pbar = tqdm(total=len(products), unit="files")

    token = _get_token(username, password)

    def _download(prod):
        _id = prod.get("Id")
        outfile = Path(outdir) / f"{_id}.zip"
        download(
            prod, username, password, outfile=outfile, show_progress=False, token=token
        )
        if show_progress:
            pbar.update(1)
        return prod, outfile

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        paths = dict(executor.map(_download, products))

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
