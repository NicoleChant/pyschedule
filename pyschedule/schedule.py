from pathlib import Path
import logging

# TODO REMOVE NUMPY 
import numpy as np
import sys
import json
import argparse
from termcolor import colored
import uuid
from glob import glob
from dataclasses import dataclass, field
from typing import Optional, Iterable, Union
import random 
import pandas as pd
import time 




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
    assembly: Optional[pd.DataFrame] = field(default=None)
    use_assembly: bool = field(default=True)
    # TODO
    # Add mindepth variable

    def __post_init__(self):
        if self.use_assembly and Path("data/assembly.txt.gz").is_file():
            self.assembly = pd.read_csv("data/assembly.txt.gz")
        else:
            self.assembly = None 

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
        
        logging.basicConfig(filename="schedule.log", level=logging.WARNING, format="%(levelname)s:%(asctime)s:%(message)s")
        

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
        files: list[Path] = [file.resolve() for file in Path(self.path).glob(f"**/*.{self.suffix}")]

        # for depth in range(1, self.maxdepth+1):
        #    presuffix = '/'.join(['*'] * depth)
        #    depth_collection = [Path(file) for file in glob(self.path + '/' + presuffix + f".{self.suffix}")]
        #    files.extend(depth_collection)
         
        print(colored(f"Apply constraints...", "blue"))
        files = self._apply_constraints(files)
        
        total_detected_files = len(files)    
        assert total_detected_files >= self.total_buckets, f"Total buckets {self.total_buckets} cannot be strictly greater than the \
                                        total number of detected files {total_detected_files}."
         
        
        print(colored(f"Total files detected: {total_detected_files}.", "green"))

        # randomize job assignment 
        if self.shuffle:
            np.random.shuffle(files)
        
        
        return files

    @staticmethod 
    def assign_tasks(tasks: list, total_buckets: int) -> list[list]:
        then = time.perf_counter()
        total = len(tasks)
        step = total // total_buckets
        remainder = total % total_buckets 
        assigned_tasks = []
        infimum = 0
        while True:
            if remainder > 0:
                supremum = infimum + step + 1
                remainder -= 1
            else:
                supremum = infimum + step
            assigned_tasks.append(tasks[infimum: supremum])

            if len(assigned_tasks) == total_buckets:
                break 

            infimum = supremum 

        now = time.perf_counter()

        # only for testing purposes
        try:
            assigned_numpy_tasks = [job.tolist() for job in np.array_split(tasks, total_buckets)]
        except Exception as err:
            print(err)
            logging.error(f"Encountered the following exception when creating the numpy array schedule: '{err}'.")
            assigned_numpy_tasks = None  
        
        if assigned_numpy_tasks is not None:
            assert assigned_tasks == assigned_numpy_tasks, "Invalid task assigment."
        
        print(colored(f"Task assignment completed within {now-then:.2f} second(s).", "green"))
        return assigned_tasks


    def schedule(self, save_json: bool = True) -> dict[int, list]:
        files = self.detect_files()
        if self.assembly is not None:
            files_df = pd.DataFrame(files, columns=["filename"])
            files_df.loc[:, "#assembly_accession"] = files_df['filename'].str.extract("(GC[AF]_\d+\.\d+)_")
            files_df = files_df.merge(self.assembly, on="#assembly_accession", how="left")

            if files_df.isna().sum().sum():
                logging.warning("NaN Values detected during assembly merging stage.")

            files_df = files_df.groupby("species_taxid").agg({"filename": lambda ds: ds.tolist()}).to_dict().pop("filename")
            
            species_taxids = list(files_df.keys())
            
            buckets = {
                    idx: {species_taxid: files_df[species_taxid] for species_taxid in bucket} for idx, bucket in enumerate(Scheduler.assign_tasks(species_taxids, self.total_buckets)) 
                    }
        else:
            buckets = {
                idx: bucket.tolist() for idx, bucket in enumerate(np.array_split(files, self.total_buckets))
            }

        schedule_id = uuid.uuid4().hex
        print(colored(f'Generated schedule id: {schedule_id}', 'green'))
    
        dest_schedule = Path(f"schedule_{schedule_id}.json")
        
        if save_json:
            with dest_schedule.open(mode="w", encoding="UTF-8") as f:
                json.dump(buckets, f, indent=4)

        print(colored(f'New schedule layout has completed.', 'green'))


        return buckets



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
    parser.add_argument("-ua", "--use_assembly", type=int, choices=[0, 1], default=1)
    args = parser.parse_args()

    # >>>>>>>>>>>>>>>>> Program Starts
    scheduler = Scheduler(**vars(args))
    scheduler.schedule()

