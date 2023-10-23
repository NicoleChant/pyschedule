# Pyschedule

## Introduction 

Pyschedule is a python package that helps you prepare job scheduling in HPC Slurm cluster. 
You may utilise it to assign the contents of a directory to a json file which you can use 
to submit array jobs by mapping the env variable SLURM_TASK_ID to the keys of the hashmap.

The need for such a small package arose from the fact that I found myself often to write 
the same boilerplate code over and over again when I wanted to submit an array job on the HPC.
This scheduler can also be combined with snakemake.


## Status

This is the first version. For any bugs you may encounter please feel free to open an issue.

There are some arguments that I have yet to work on by adding further restrictions on the inclusion of files. 
Nonetheless it's currently on my todo list.

*By Channi*

## Usage

First you need to clone the repo and install via pip

```
git clone git@github.com:NicoleChant/pyschedule.git
```


```
pip install .
```

Thereafter, you may use the command line utility by running

```
sassign <dir_path> --total_buckets 10 --suffix '.fa'
```

There are multiple command line arguments you can use to filter the files that lie within the specified path.

