import os
import shutil
import tempfile
from contextlib import closing
from os.path import getsize
import threading
from multiprocessing.pool import ThreadPool

import requests
from tqdm import tqdm


pbar = tqdm()

def download(uid, username, password, outfile=None, workdir=None):
    """Downloads a file to the given location.
    Cycles through credentials if multiple (user, password) sets are given.
    This function stores unfinished downloads in the given working directory.
    Run with `shadow: 'shallow'` to prevent artefacts.
    Parameters:
        url (str):
            URL to download from.
        outfile (str or Path):
            Output filename.
        credentials ((username, password) or iterable of (username, password) pairs):
            Credentials to use while downloading.
        workdir (str or Path):
            Path where incomplete downloads are stored.
    """
    if outfile==None:
        outfile=f'{uid}.zip'

    token_data = {
    'client_id': 'CLOUDFERRO_PUBLIC',
    'username': username,
    'password': password,
    'grant_type': 'password'
    }
    url = f'https://zipper.creodias.eu/download/{uid}'

    token = requests.post('https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token',
                             data=token_data).json()['access_token']

    if workdir is None:
        workdir = os.getcwd()

    temp = tempfile.NamedTemporaryFile(delete=False, dir=workdir)
    temp.close()
    local_path = temp.name

    url += f'?token={token}'
    #with requests.get(url, stream=True, headers={'token': token}) as resp:
    _download_raw_data(url, local_path)
    shutil.move(local_path, outfile)

def download_list(uids, username, password, outdir=None, workdir=None, threads=3):
    pool = ThreadPool(threads)
    #for uid in uids:
    download_lambda = lambda x : download(x, username, password, outfile=None, workdir=None)
    pool.map(download_lambda, uids)
        #threading.Thread(target=download, args=)
        #download(uid, username, password, outdir)


def _download_raw_data(url, path):
    already_downloaded_bytes = getsize(path)
    downloaded_bytes = 0
    headers = {"Range": "bytes={}-".format(already_downloaded_bytes)}
    with closing(
            requests.get(url, stream=True, timeout=10)
    ) as r, tqdm(
        unit = 'B',
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



if __name__ == "__main__":
    # uid = 'c0e762e2-fb27-5f9e-9cd0-7883d99c3b81'#'db0c8ef3-8ec0-5185-a537-812dad3c58f8'
    # download(uid, outfile="test.zip", username='anbr@dhigroup.com', password='aCAtX.D.-qn9vnkGRFDm')

    uids = ['c0e762e2-fb27-5f9e-9cd0-7883d99c3b81',
            'dfe2b8a6-6b95-5f47-8d36-44806e509d8d',
            '4f6c38e4-a3f8-51b9-8106-4d0a4652acc7',
            'd0e0e0b5-5008-5096-8726-63c242c63511',
            '68959a6d-0793-5df4-8b0c-8daf49b001ba',
            'c9f42d5b-3238-5ee9-8c2d-06b4e3046330']
    download_list(uids, username='anbr@dhigroup.com', password='aCAtX.D.-qn9vnkGRFDm', threads=6)
