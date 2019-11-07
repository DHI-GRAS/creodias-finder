import shutil
import contextlib
from pathlib import Path
import concurrent.futures

import requests
from tqdm import tqdm

DOWNLOAD_URL = 'https://zipper.creodias.eu/download'
TOKEN_URL = 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token'


def _get_token(username, password):
    token_data = {
        'client_id': 'CLOUDFERRO_PUBLIC',
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    response = requests.post(TOKEN_URL, data=token_data).json()
    try:
        return response['access_token']
    except KeyError:
        raise RuntimeError(f'Unable to get token. Response was {response}')


def download(uid, username, password, outfile, show_progress=True):
    """Download a file from CreoDIAS to the given location

    Parameters
    ----------
    uid:
        CreoDIAS UID to download
    username:
        CreoDIAS username (email address)
    password:
        CreoDIAS password
    outfile:
        path to output file
    """
    token = _get_token(username, password)
    url = f'{DOWNLOAD_URL}/{uid}?token={token}'
    _download_raw_data(url, outfile, show_progress)


def download_list(uids, username, password, outdir, threads=1, show_progress=True):
    """Downloads a list of UIDS

    Parameters
    ----------
    uids:
        A list of UIDs
    username:
        CreoDIAS username (email address)
    password:
        CreoDIAS password
    outdir:
        output direcotry
    threads:
        number of simultaneous downloads

    Returns
    -------
    dict
        mapping uids to paths to downloaded files
    """
    if show_progress:
        pbar =  tqdm(total = len(uids), unit='files')
    def _download(uid):
        outfile = Path(outdir) / f'{uid}.zip'
        download(uid, username, password, outfile=outfile, show_progress=False)
        if show_progress:
            pbar.update(1)
        return uid, outfile

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        paths = dict(executor.map(_download, uids))

    return paths


def _download_raw_data(url, outfile, show_progress=False):
    """Downloads data from url to outfile.incomplete and then moves to outfile"""
    outfile_temp = str(outfile) + '.incomplete'
    try:
        with contextlib.ExitStack() as stack:
            downloaded_bytes = 0
            req = stack.enter_context(requests.get(url, stream=True, timeout=10))
            progress = stack.enter_context(tqdm(unit='B', unit_scale=True, disable=not show_progress))
            chunk_size = 2 ** 20  # download in 1 MB chunks
            fout = stack.enter_context(open(outfile_temp, 'wb'))
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
