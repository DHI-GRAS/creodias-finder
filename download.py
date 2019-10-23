import os
import shutil
import tempfile
from contextlib import closing
from os.path import getsize

import requests
from tqdm import tqdm


pbar = tqdm()

def download(url, outfile, credentials=None, workdir=None):
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

    token_data = {
    'client_id': 'CLOUDFERRO_PUBLIC',
    'username': 'anbr@dhigroup.com',
    'password': 'aCAtX.D.-qn9vnkGRFDm',
    'grant_type': 'password'
    }

    token = requests.post('https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token',
                             data=token_data).json()['access_token']

    if workdir is None:
        workdir = os.getcwd()

    temp = tempfile.NamedTemporaryFile(delete=False, dir=workdir)
    temp.close()
    local_path = temp.name

    url += f'?token={token}'
    #with requests.get(url, stream=True, headers={'token': token}) as resp:
    _download_raw_data(url, local_path, 400)
    shutil.move(local_path, outfile)

def _download_raw_data(url, path, file_size):
    already_downloaded_bytes = getsize(path)
    downloaded_bytes = 0
    headers = {"Range": "bytes={}-".format(already_downloaded_bytes)}
    with closing(
            requests.get(url, stream=True)
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
    url = "https://zipper.creodias.eu/download/db0c8ef3-8ec0-5185-a537-812dad3c58f8"
    download(url, outfile="test.zip")