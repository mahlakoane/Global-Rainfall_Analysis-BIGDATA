# ELEN4020 - PROJECT
**Group Members:**
- Bohlale Mahlakoane - 1352926
- Tshegofatso Kale - 1600916
- Lungelo Chala - 1586897

## Global Rainfall Analysis with NETCDF Files
This program is written in Python and uses the MPI4PY library to distribute work across multiple message passing interface nodes. This is done to generate box and whisker diagrams from data stored in NetCDF files.

## Compile Instructions
The folowing requirements must be met in order to successfully execute the program:

* The code must run on the Wits EIE jaguar cluster.
* Python >= 3.8 must be installed along with pip and virualenv.
* The data sould be stored in the `/data` directory.

Once all the requirements are met, one must create a python virtual environment and load dependencies from the `requirements.txt` file as follows:

```bash
virtualenv -p /usr/bin/python3.8 Environment
source Environment/bin/activate
pip3 install -r requirements.txt
```

The user then runs the following command in order to execute the program:

```bash
srun -n15 --mpi=pmi2 python3 projectCode.py 4 2001 2015 v2020 v2020_10
```

In the above example, the user the executing the program with 15 nodes as evidenced by the -n15 flag. The `projectCode.py` script accepts 5 arguments. These are:

* The number of threads each node will use to execute the code (4 in the above example).
* The start and end years of the data to be processed (2001 - 2015 in the above example).
* The version of the data to be processed (v2020 in the above example).
* What is appended to the file before the year i.e full_data_daily_`v2020_10`_XXXX.nc (v2020_10 in the above example).


