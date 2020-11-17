# NASAP_CHIRPS_WTH
Routines to download weather data from the NASA POWER application and merge with CHIRPS dataset to produce WTH files. WTH files are the native format input for DSSAT.

These routines are intended to download weather data from the NASA POWER application using OPeNDAP (NP.py),
and then merge the NASA POWER data with CHIRPS data in a DSSAT output format (DSSAT_WTH.py).
See "Test_input.zip" for an example of input files (CHIRPS files are not included).

Inputs:
1. A CSV file (.CSV) with the following columns: "ID", "Latitude", "Longitude", "nasapid", "LatNP", and "LonNP"
2. A directory with the CHIRPS files.

Outputs:
1. "DSSAT0" directory: WTH files without quality control for missing values.
2. "DSSAT" directory: WTH files with quality control.
3. "dfNASAP.pkl": Python dataframe with the NASA POWER data.
4. "dfCHIRPS.pkl": Python dataframe with the CHIRPS data.
5. "XY_Points.shp" dataset: Point-vector file of the input CSV file.

How to run:
Create and configure the input files and directories in the "WTH_main.py" file and then run it.
Application runs on Python 3.8.5 version. It is tested in Windows OS environment.
