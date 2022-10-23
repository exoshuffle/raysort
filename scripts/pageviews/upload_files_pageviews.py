import gzip
import shutil

import boto3
import io
import ray
import requests


def upload_files(i, j, k):
    url_ending = f"/pagecounts-20160{i}{j:02d}-{k:02d}0000.gz"
    title_ending = f"/pagecounts-20160{i}{j:02d}-{k:02d}0000"

    url = f"https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-0{str(i)}{url_ending}"

    print(url)
    r = requests.get(url, allow_redirects=True)

    compressedFile = io.BytesIO()
    compressedFile.write(r.content)
    compressedFile.seek(0)

    decompressedFile = gzip.GzipFile(fileobj=compressedFile, mode="rb")

    s3_client = boto3.client("s3")

    s3_client.upload_fileobj(
        decompressedFile,
        "lsf-berkeley-edu",
        "wikimedia{}".format(title_ending),
    )


def main():
    """Upload data for months 1, 3, and 5"""
    for i in range(1, 6, 2):
        for j in range(1, 32):
            for k in range(0, 24):
                upload_files(i, j, k)


if __name__ == "__main__":
    main()
