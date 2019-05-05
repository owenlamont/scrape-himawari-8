import argparse
import datetime
import os
import shutil
from pathlib import Path
from typing import List, Set, Optional, Dict

import pandas as pd
import requests
import urllib3
from tqdm import tqdm


def main(output_folder: Path, max_retries: int, look_back_days: int):
    img_file_list: List[str] = output_folder.glob("*.jpg")
    img_file_list = [os.path.basename(file_path) for file_path in img_file_list]
    img_file_set: Set[str] = set(img_file_list)

    end_time = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0, second=0, minute=0
    )
    start_time = end_time - datetime.timedelta(days=look_back_days)

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
                while not succeeded and try_counter < max_retries:
                    try:
                        resume_header: Optional[Dict[str, str]] = None
                        if os.path.isfile(save_file_path):
                            offset = os.path.getsize(save_file_path)
                            resume_header = {"Range": f"bytes={offset}-"}

                        response = requests.get(url=url, headers=resume_header, stream=True, timeout=0.5)
                        if response.status_code in [requests.codes.ok, requests.codes.partial_content]:
                            if resume_header is None:
                                with open(save_file_path, "wb") as out_file:
                                    shutil.copyfileobj(response.raw, out_file)
                            else:
                                with open(save_file_path, "ab") as out_file:
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
                        pass
                # Delete any partially downloaded files that didn't complete
                if not succeeded and os.path.isfile(save_file_path):
                    try:
                        os.remove(save_file_path)
                    except:
                        None


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument(
        "output_folder", help="The name of the folder to save the images to.", type=str
    )
    parser.add_argument(
        "-max_retries",
        help="The maximum number of times to attempt to download an image",
        type=int,
        default=10,
    )
    parser.add_argument(
        "-look_back_days",
        help="The number of days before present to search back from",
        type=int,
        default=21,
    )
    args = parser.parse_args()
    main(
        output_folder=Path(args.output_folder),
        max_retries=args.max_retries,
        look_back_days=args.look_back_days,
    )
