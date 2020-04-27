from pathlib import Path
from creodias_finder.download import download, download_list


def test_download(uid, username, password, tmp_path, workdir):
    create_workdir_if_not_exists(workdir)

    outfile = tmp_path
    download(uid, username, password, outfile=outfile, workdir=workdir)
    write_path = list(outfile.glob("*"))[0]
    assert outfile / f"{uid}.zip" == Path(write_path)


def test_download_list(uids, username, password, tmp_path):
    outfile, workdir = tmp_path / "outfiles", tmp_path / "workdir"
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


def create_workdir_if_not_exists(path):
    if path:
        print("THIS IS P ", path)
        path = Path(path)
        if not path.is_dir():
            path.mkdir()
