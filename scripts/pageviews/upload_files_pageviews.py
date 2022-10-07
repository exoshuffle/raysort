import requests
import boto3
import gzip
import shutil


def upload_files():
    # Upload data for months 1, 3, and 5
    for i in range(1, 6, 2):
        for j in range(1, 32):
            for k in range(0, 24):
                if j < 10 and k < 10:
                    url_ending = (
                        "/pagecounts-20160"
                        + str(i)
                        + "0"
                        + str(j)
                        + "-0"
                        + str(k)
                        + "0000.gz"
                    )
                    title_ending = (
                        "/pagecounts-20160"
                        + str(i)
                        + "0"
                        + str(j)
                        + "-0"
                        + str(k)
                        + "0000"
                    )
                elif j < 10 and k >= 10:
                    url_ending = (
                        "/pagecounts-20160"
                        + str(i)
                        + "0"
                        + str(j)
                        + "-"
                        + str(k)
                        + "0000.gz"
                    )
                    title_ending = (
                        "/pagecounts-20160"
                        + str(i)
                        + "0"
                        + str(j)
                        + "-"
                        + str(k)
                        + "0000"
                    )
                elif j >= 10 and k < 10:
                    url_ending = (
                        "/pagecounts-20160"
                        + str(i)
                        + str(j)
                        + "-0"
                        + str(k)
                        + "0000.gz"
                    )
                    title_ending = (
                        "/pagecounts-20160" + str(i) + str(j) + "-0" + str(k) + "0000"
                    )
                else:
                    url_ending = (
                        "/pagecounts-20160" + str(i) + str(j) + "-" + str(k) + "0000.gz"
                    )
                    title_ending = (
                        "/pagecounts-20160" + str(i) + str(j) + "-" + str(k) + "0000"
                    )

                url = (
                    "https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-0"
                    + str(i)
                    + url_ending
                )
                print(url)
                r = requests.get(url, allow_redirects=True)
                filename_gz = "file.gz"
                filename = "file.txt"
                s3_client = boto3.client("s3")
                with open(filename_gz, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024):

                        if chunk:
                            f.write(chunk)

                with gzip.open(filename_gz, "rb") as f_in:
                    with open(filename, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                s3_client.upload_file(
                    "file.txt",
                    "lsf-berkeley-edu",
                    "wikimedia{}".format(title_ending),
                )

                f.close()


def main():
    upload_files()


if __name__ == "__main__":
    main()
