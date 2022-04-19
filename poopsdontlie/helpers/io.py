import requests
from io import BytesIO
from tqdm.auto import tqdm


def download_file_with_progressbar(url, leave=True):
    print(f'Downloading {url}')

    headers = {
            'accept-encoding': 'gzip',
    }
    res = requests.get(url, headers=headers, stream=True)
    res.raise_for_status()
    size = int(res.headers.get('content-length', 0))
    bsize = 4096
    pbar = tqdm(total=size, unit='iB', unit_scale=True, leave=leave)
    retval = BytesIO()
    for data in res.iter_content(bsize):
        pbar.update(len(data))
        retval.write(data)
    pbar.close()
    retval.seek(0)

    return retval
