from pathlib import Path
from creodias_finder.download import download, download_list


def test_download(uid, username, password, tmp_path):
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    download(uid, username, password, outfile=tmp_path, workdir=workdir)
    expected_path = Path(list(tmp_path.glob("*"))[0])
    assert tmp_path / f"{uid}.zip" == expected_path


def test_download_list(uids, username, password, tmp_path):
    outfile = tmp_path / "outfiles"
    workdir = tmp_path / "workdir"
    outfile.mkdir()
    workdir.mkdir()
    download_list(
        uids,
        username,
        password,
        outdir=outfile,
        workdir=workdir,
        threads=max(10, len(uids)),
    )

    created_files = set()
    for glob_file in outfile.glob("*"):
        created_files.add(str(glob_file))
    file_assertions = set()
    for uid in uids:
        file_assertions.add(str(outfile / f"{uid}.zip"))

    assert created_files == file_assertions
