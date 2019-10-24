import os
import shutil
import tempfile
from contextlib import closing
from os.path import getsize
import threading
from multiprocessing.pool import ThreadPool
from pathlib import Path

import requests
from tqdm import tqdm


pbar = tqdm()


def download(uid, username, password, outfile=None, workdir=None):
    """Downloads a file to the given location.
    This function stores unfinished downloads in the given working directory.
    Parameters:
        :param uid: CREO DIAS UID to download.
        :param username: Username.
        :param password: Password.
        :param workdir: Path where incomplete downloads are stored.
        :param outfile: Location or name of output file. If a location is provided, the outfile will
        be named as [UID].zip. Location can be either absolute or relative.
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
    if not path:
        path = Path(os.getcwd())
    else:
        path = Path(path)
    return path


def download_list(uids, username, password, outdir=None, workdir=None, threads=3):
    """Downloads a list of UIDS
    File names are [UID].zip
    :param uids: A list of UIDs.
    :param username: Username.
    :param password: Password.
    :param outdir: Output direcotry.
    :param workdir: Storage of temporary files
    :param threads: Number of simultaneous downloads.
    """
    pool = ThreadPool(threads)
    download_lambda = lambda x: download(x, username, password, outfile=outdir, workdir=workdir)
    pool.map(download_lambda, uids)


def _download_raw_data(url, path):
    already_downloaded_bytes = getsize(path)
    downloaded_bytes = 0
    headers = {"Range": "bytes={}-".format(already_downloaded_bytes)}
    with closing(
            requests.get(url, stream=True, timeout=10)
    ) as r, tqdm(
        unit='B',
        unit_scale=True
    ) as progress:
        chunk_size = 2 ** 20  # download in 1 MB chunks
        i = 0
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    progress.update(len(chunk))
                    downloaded_bytes += len(chunk)
                    i += 1
        # Return the number of bytes downloaded
        return downloaded_bytes
