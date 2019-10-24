import os
from pathlib import Path
from creodias_finder.download import download


def test_download(uid, username, password, tmp_path):
    outfile, workdir = tmp_path / "outfiles", tmp_path / "workdir"
    outfile.mkdir()
    workdir.mkdir()
    download(uid, username, password, outfile=outfile, workdir=workdir)
    write_path = list(outfile.glob("*"))[0]
    assert outfile / f'{uid}.zip' == Path(write_path)

