#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:        WTH_main.py
# Purpose:     To invoke the necessary functions to build weather files for the native DSSAT format (.WTH)
# Author:      Oscar Castillo
#              ocastilloromero@ufl.edu
# Created:     11/01/2020
# Runs in Python 3.8.5
##Columns of the CSV input file:
#ID,Latitude,Longitude,nasapid,LatNP,LonNP
#-------------------------------------------------------------------------------

from multiprocessing import Process
from NP import nasap_gen
from CHIRPS import chirps
from DSSAT_WTH import nasachirps
import os

in_file = "C:\\Work\\Test\\XYpoints.csv"
ID = "ID"
NASAP_ID = "nasapid"
in_chirps = "C:\\Work\\Test\\in_chirps"
sy, sm, sd, ey, em, ed = (2020, 10, 29, 2020, 10, 31)
out_dir = "C:\\Work\\Test\\Output"

if __name__=='__main__':
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    p1 = Process(target=nasap_gen, args=(in_file, out_dir, sy, sm, sd, ey, em, ed))
    p1.start()
    p2 = Process(target=chirps, args=(in_chirps, in_file, out_dir, ID))
    p2.start()
    p1.join()
    p2.join()
    nasap_file = out_dir + "\\dfNASAP.pkl"
    chirps_file = out_dir + "\\dfCHIRPS.pkl"
    p3 = Process(target=nasachirps, args=(in_file, nasap_file, chirps_file, out_dir))
    p3.start()
    p3.join()
