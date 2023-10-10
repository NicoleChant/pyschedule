from pathlib import Path
import numpy as np
import sys
import json
import argparse
from termcolor import colored
import uuid
from glob import glob
from dataclasses import dataclass, field
from typing import Optional, Iterable

@dataclass(slots=True, kw_only=True)
class Scheduler:
    
    path: str
    total_buckets: int
    suffix: str
    maxdepth: int = 1
    shuffle: bool = False
    starting_constraint: Optional[str] = field(default=None)
    ending_constraint: Optional[str] = field(default=None)
    existence_constraint: Optional[str] = field(default=None)
    parent_constraint: Optional[Iterable[str]] = field(default=None)
    
    # TODO
    # Add mindepth variable

    def __post_init__(self):
        if self.suffix.startswith("."):
            self.suffix = self.suffix[1:]
    
        if not self.path.endswith("/"):
            self.path += "/"

        assert isinstance(self.maxdepth, int), f"Maxdepth {self.maxdepth} must be an int."
        assert isinstance(self.total_buckets, int), f"Total buckets {self.total_buckets} must be an int."
        
        print(colored("Detected constraints.", "blue"))
        print(colored(f"Ending constraint: '{self.ending_constraint}'", "blue"))
        print(colored(f"Starting constraint: '{self.starting_constraint}'", "blue"))
        print(colored(f"Parent constraint: '{self.parent_constraint}'", "blue"))
    
        


    def _apply_constraints(self, files: list[Path]) -> list[str]:
        """Removes files failed to pass constraint check and returns a filtered list."""
        
        if self.ending_constraint:
            files = filter(lambda file: file.stem.endswith(self.ending_constraint), files)
        
        if self.starting_constraint:
            files = filter(lambda file: file.name.startswith(self.starting_constraint), files)
            
        if self.existence_constraint:
            files = filter(lambda file: self.existence_constraint in file.name, files)

        if self.parent_constraint:
            files = filter(lambda file: file.parent.name in self.parent_constraint, files)

        return [str(file) for file in files]

    
    def detect_files(self) -> list[str]:
        print(colored("Please wait while the files are being detected...", "blue"))
        files: list[Path] = [file for file in Path(self.path).glob(f"**/*.{self.suffix}")]

        # for depth in range(1, self.maxdepth+1):
        #    presuffix = '/'.join(['*'] * depth)
        #    depth_collection = [Path(file) for file in glob(self.path + '/' + presuffix + f".{self.suffix}")]
        #    files.extend(depth_collection)
         
        print(colored(f"Apply constraints...", "blue"))
        files = self._apply_constraints(files)
        
        total_detected_files = len(files)    
        assert total_detected_files >= self.total_buckets, f"Total buckets {self.total_buckets} cannot be strictly greater than the \
                                        total number of detected files {total_detected_files}."
         

        # randomize job assignment 
        if self.shuffle:
            np.random.shuffle(files)

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
    parser.add_argument("-tb", "--total_buckets", type=int, default=10)
    parser.add_argument("-ex", "--existence_constraint", type=str, default=None)
    parser.add_argument("-sc", "--starting_constraint", type=str, default=None)
    parser.add_argument("-ec", "--ending_constraint", type=str, default=None)
    parser.add_argument("-pc", "--parent_constraint", nargs='+', type=str, default=None)

    args = parser.parse_args()

    # >>>>>>>>>>>>>>>>> Program Starts
    scheduler = Scheduler(**vars(args))
    scheduler.schedule()

