# Pyschedule


Pyschedule is a python package that helps you prepare job scheduling in HPC Slurm cluster. 
You may utilise it to assign the contents of a directory to a json file which you can use 
to submit array jobs.

This is the first version. For any bugs you may encounter please feel free to open an issue.

There are some arguments that I have yet to work on by adding further restrictions on the inclusion of files. 
Nonetheless it's currently on my todo list.

*Channi*

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

