# NASAP_CHIRPS_WTH
Routines to download weather data from the NASA POWER application and merge with CHIRPS dataset to produce WTH files.

These routines are intended to first download weather data from the NASA POWER application (NASAP.py),
and secondly, to merge the NASA POWER data with CHIRPS dataset in a DSSAT output format (DSSAT_WTH.py).
See "Test.zip" for an example of input files (CHIRPS files not included).

Inputs:
1. A plain text file (.TXT) with three columns: "ID", "Latitude", and "Longitude"
2. A point shapefile containing the attributes in the plain text file.
3. A directory with the CHIRPS files.

Outputs:
1. "NASAP" directory
2. "DSSAT0" directory
3. "DSSAT" directory

Notes:
"NASAP.py" runs in Python 2.7

"DSSAT_WTH.py" runs in Python 3.7

In later versions the "NASAP.py" will be upgraded to Python 3.
