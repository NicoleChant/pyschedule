from pathlib import Path
import numpy as np
import sys
import json
import argparse
from termcolor import colored
import uuid
import importlib
from glob import glob
from dataclasses import dataclass, field
from typing import Optional

@dataclass(slots=True, kw_only=True)
class Scheduler:
    
    path: str
    total_buckets: int
    suffix: str
    maxdepth: int = 1
    shuffle: bool = False
    fpath: Optional[str] = field(default=None)
    fname: Optional[str] = field(default=None)
    

    def __post_init__(self):
        if self.suffix.startswith("."):
            self.suffix = self.suffix[1:]
    
        if self.fpath and self.fname:
            importlib.import_module(self.fpath, package=None)

        if not self.path.endswith("/"):
            self.path += "/"
    

    def detect_files(self) -> list[str]:
        files = [file for file in glob(self.path + '/'.join(['*'] * self.maxdepth) + f".{self.suffix}")]
        total_detected_files = len(files)    
        assert total_detected_files >= self.total_buckets, f"Total buckets {total_buckets} cannot be strictly greater than the \
                                        total number of detected files {total_detected_files}."
        if self.shuffle:
            np.random.shuffle(files)

        print(colored(f'Total files detected {len(files)}', 'magenta'))
        
        return files

    def schedule(self):
        files = self.detect_files()

        buckets = {
            idx: bucket.tolist() for idx, bucket in enumerate(np.array_split(files, self.total_buckets))
        }

        schedule_id = uuid.uuid4()
        print(colored(f'Generated schedule id: {schedule_id}', 'green'))
    
        dest_schedule = Path(f"schedule_{schedule_id}.json")
        
        with dest_schedule.open(mode="w", encoding="UTF-8") as f:
            json.dump(buckets, f, indent=4)

        print(colored(f'New schedule layout has completed.', 'green'))



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, default="")
    parser.add_argument("-s", "--suffix", type=str, default="fna")
    parser.add_argument("-md", "--maxdepth", type=int, default=1)
    parser.add_argument("-fp", "--fpath", type=str)
    parser.add_argument("-fn", "--fname", type=str)
    parser.add_argument("-tb", "--total_buckets", type=int, default=10)
    
    args = parser.parse_args()
    suffix = args.suffix
    path = args.path
    fpath = args.fpath
    fname = args.fname
    maxdepth = args.maxdepth
    total_buckets = args.total_buckets

    # >>>>>>>>>>>>>>>>> Program Starts
    scheduler = Scheduler(**vars(args))
    scheduler.schedule()

