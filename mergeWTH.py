#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:        mergeWTH.py
# Purpose:     To merge two DSSAT weather files (.WTH)
# Author:      Oscar Castillo
#              ocastilloromero@ufl.edu
# Created:     11/01/2020
# Runs in Python 3.8.5
#-------------------------------------------------------------------------------

import os
import shutil

#Sample
in_dir1 = r"C:\Users\oscar\Documents\Work\NASAPCHIRPS\NASAP_CHIRPS_WTH-develop\Test_output\DSSAT1" #First directory
in_dir2 = r"C:\Users\oscar\Documents\Work\NASAPCHIRPS\NASAP_CHIRPS_WTH-develop\Test_output\DSSAT2" #Second directory to append to the first directory
out_dir = r"C:\Users\oscar\Documents\Work\NASAPCHIRPS\NASAP_CHIRPS_WTH-develop\Test_output\DSSAT3" #Output directory

def mergeWTH(in_dir1, in_dir2, out_dir):
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    wth_dir2 = os.listdir(in_dir2)

    for wth_file1 in os.listdir(in_dir1):
        if wth_file1.endswith(".WTH"):
            if wth_file1 in wth_dir2:
                #To make a copy of the first file
                shutil.copy(in_dir1 + "\\" + wth_file1, out_dir + "\\" + wth_file1)
                #To open a second file
                with open(in_dir2 + "\\" + wth_file1, "r") as wth2:
                    data2 = [line for line in wth2.readlines() if line.strip()][4:]

                #To open the copied file and append the second file
                with open(out_dir + "\\" + wth_file1, "r+") as wth1:
                    data1 = [line for line in wth1.readlines() if line.strip()]
                    wth1.writelines(data2)
            else:
                print("The file ", wth_file1, " will not be updated.")
