import argparse
import datetime
import os
import shutil
from pathlib import Path
from typing import List, Set

import pandas as pd
import requests
import urllib3
from tqdm import tqdm

LOOK_BACK_DAYS = 21
MAX_RETRIES: int = 10


def main(output_folder: Path):
    img_file_list: List[str] = output_folder.glob("*.jpg")
    img_file_list = [os.path.basename(file_path) for file_path in img_file_list]
    img_file_set: Set[str] = set(img_file_list)

    end_time = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0, second=0, minute=0)
    start_time = end_time - datetime.timedelta(days=LOOK_BACK_DAYS)

    date_range = pd.date_range(start=start_time, end=end_time, freq="10T")

    with tqdm(total=len(date_range)) as pbar:
        for date in date_range:
            pbar.update(1)
            file_name: str = f"{date.strftime('%Y%m%d%H%M')}.jpg"
            if file_name not in img_file_set:
                try_counter: int = 0
                succeeded: bool = False
                url = f"http://rammb.cira.colostate.edu/ramsdis/online/images/hi_res/himawari-8/full_disk_ahi_true_color/full_disk_ahi_true_color_{date.strftime('%Y%m%d%H%M%S')}.jpg"
                save_file_path: Path = output_folder / file_name
                while not succeeded and try_counter < MAX_RETRIES:
                    try:
                        response = requests.get(url, stream=True, timeout=0.5)
                        if response.status_code == 200:
                            with open(save_file_path, "wb") as out_file:
                                shutil.copyfileobj(response.raw, out_file)
                                succeeded = True
                        else:
                            try_counter += 1
                        del response
                    except (
                        requests.exceptions.RequestException,
                        urllib3.exceptions.ProtocolError,
                        urllib3.exceptions.ReadTimeoutError,
                        ConnectionResetError,
                    ):
                        try_counter += 1
                # Delete any partially downloaded files that didn't complete
                if not succeeded and os.path.isfile(save_file_path):
                    try:
                        os.remove(save_file_path)
                    except:
                        None


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument(
        "output_folder", help="The name of the folder to save the images to."
    )
    args = parser.parse_args()
    main(Path(args.output_folder))
