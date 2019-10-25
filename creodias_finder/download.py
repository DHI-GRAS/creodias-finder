import os
import shutil
import tempfile
from contextlib import closing
from multiprocessing.pool import ThreadPool
from pathlib import Path

import requests
from tqdm import tqdm


PBAR = tqdm()


def download(uid, username, password, outfile=None, workdir=None):
    """Downloads a file to the given location.
    This function stores unfinished downloads in the given working directory.

    Parameters
    ----------
    uid:
        CREO DIAS UID to download.
    username:
        Username.
    password:
        Password.
    outfile:
        Path where incomplete downloads are stored.
    workdir:
        Location or name of output file. If a location is provided, the outfile will
        be named as [UID].zip. Location can be either absolute or relative.

    Returns
    -------
    None
    """
    outfile = _format_path(outfile)
    if os.path.isdir(outfile):
        outfile /= f'{uid}.zip'

    token_data = {
        'client_id': 'CLOUDFERRO_PUBLIC',
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    url = f'https://zipper.creodias.eu/download/{uid}'

    token = requests.post('https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token',
                          data=token_data).json()['access_token']

    workdir = _format_path(workdir)

    temp = tempfile.NamedTemporaryFile(delete=False, dir=workdir)
    temp.close()
    local_path = temp.name

    url += f'?token={token}'
    _download_raw_data(url, local_path)
    shutil.move(local_path, outfile)


def _format_path(path):
    """Change path to a Path object"""
    if not path:
        path = Path(os.getcwd())
    else:
        path = Path(path)
    return path


def download_list(uids, username, password, outdir=None, workdir=None, threads=3):
    """Downloads a list of UIDS

    Parameters
    ----------
    uids:
        A list of UIDs.
    username:
        Username.
    password:
        Password.
    outdir:
        Output direcotry.
    workdir:
        Storage of temporary files
    threads:
        Number of simultaneous downloads.

    Returns
    -------
    None
    """
    pool = ThreadPool(threads)
    download_lambda = lambda x: download(x, username, password, outfile=outdir, workdir=workdir)
    pool.map(download_lambda, uids)


def _download_raw_data(url, output_path):
    """Downloads data from url to output_path"""
    downloaded_bytes = 0
    with closing(
            requests.get(url, stream=True, timeout=10)
    ) as req, tqdm(
        unit='B',
        unit_scale=True
    ) as progress:
        chunk_size = 2 ** 20  # download in 1 MB chunks
        i = 0
        with open(output_path, 'wb') as outfile:
            for chunk in req.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    outfile.write(chunk)
                    progress.update(len(chunk))
                    downloaded_bytes += len(chunk)
                    i += 1
        # Return the number of bytes downloaded
        return downloaded_bytes
