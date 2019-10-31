import shutil
from pathlib import Path
import concurrent.futures

import requests
from tqdm import tqdm

DOWNLOAD_URL = 'https://zipper.creodias.eu/download'
TOKEN_URL = 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token',


def download(uid, username, password, outfile):
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
    token_data = {
        'client_id': 'CLOUDFERRO_PUBLIC',
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    token = requests.post(TOKEN_URL, data=token_data).json()['access_token']
    url = f'{DOWNLOAD_URL}/{uid}?token={token}'
    _download_raw_data(url, outfile)


def download_list(uids, username, password, outdir, threads=1):
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
    def _download(uid):
        outfile = Path(outdir) / '{uuid}.zip'
        download(uid, username, password, outfile=outfile)
        return uid, outfile

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        paths = dict(executor.map(_download, uids))

    return paths


def _download_raw_data(url, outfile):
    """Downloads data from url to outfile.incomplete and then moves to outfile"""
    outfile_temp = str(outfile) + '.incomplete'
    try:
        downloaded_bytes = 0
        with requests.get(url, stream=True, timeout=10) as req:
            with tqdm(unit='B', unit_scale=True) as progress:
                chunk_size = 2 ** 20  # download in 1 MB chunks
                with open(outfile_temp, 'wb') as fout:
                    for chunk in req.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            fout.write(chunk)
                            progress.update(len(chunk))
                            downloaded_bytes += len(chunk)
    finally:
        try:
            Path(outfile_temp).unlink()
        except OSError:
            pass
    shutil.move(outfile_temp, outfile)
