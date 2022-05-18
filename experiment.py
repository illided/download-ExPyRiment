import argparse
import hashlib
import os
import random
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path
from timeit import default_timer as timer
from typing import Dict, List, Optional

import requests
from tqdm import tqdm


def run(f, my_iter, n_threads, *args):
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        results = list(tqdm(executor.map(f, my_iter, *[repeat(arg) for arg in args]), total=len(my_iter)))
    return results


def process_image(url: str, dest_dir: str) -> Optional[Dict[str, float]]:
    url = url.strip()
    measure_dict = {}

    start = timer()
    pic_name = hashlib.md5(url.encode("utf-8")).hexdigest() + '.png'
    dest_filepath = os.path.join(dest_dir, pic_name)
    measure_dict['md5 hashing'] = timer() - start

    image_file = open(dest_filepath, 'wb')

    def fail():
        image_file.close()
        os.remove(dest_filepath)

    start = timer()
    try:
        responce = requests.get(url, stream=True)
    except requests.exceptions.RequestException:
        fail()
        return None

    if not responce.ok:
        fail()
        return None

    image = responce.content
    measure_dict['download'] = timer() - start
    responce.close()

    start = timer()
    image_file.write(image)
    measure_dict['save'] = timer() - start

    image_file.close()

    return measure_dict


def run_experiment(n_threads: int, dest_dir: str, links_filepath: str, limit=None) -> Dict[str, float]:
    links_file = open(links_filepath)
    links = links_file.readlines()
    links_file.close()

    random.shuffle(links)

    if limit is not None:
        links = links[:limit]

    Path(dest_dir).mkdir(exist_ok=True, parents=True)

    results: List[Dict[str, float]] = run(process_image, links, n_threads, dest_dir)
    results = [r for r in results if r]
    print(f"Downloaded successfully: {len(results)} out of {len(links)}")

    sum_dict = results[0].copy()
    for res in results[1:]:
        for key, value in res.items():
            sum_dict[key] += value
    for key in sum_dict:
        sum_dict[key] /= len(results)
    return sum_dict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('img_dir')
    parser.add_argument('max_workers', type=int, default=10)
    parser.add_argument('limit', type=Optional[int], default=None)

    args = parser.parse_args()
    res = run_experiment(
        n_threads=args.max_workers,
        links_filepath="links.txt",
        dest_dir=args.img_dir,
        limit=args.limit
    )
    print(res)


if __name__ == "__main__":
    main()
