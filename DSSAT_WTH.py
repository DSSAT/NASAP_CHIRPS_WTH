#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:        DSSAT_WTH.py
# Purpose:     To create weather files in DSSAT format. This version uses the native grid size of nasap to create the 5-arc min points.
#              Dataframes for CHIRPS and NASAP must be provided.
# Author:      Oscar Castillo
#              ocastilloromero@ufl.edu
# Created:     11/01/2020
# Runs in Python 3.8.5
# Example:
# nasachirps(in_file, nasap_file, chirps_file, out_dir, NASAP_ID = "nasapid", ID = "ID")
# nasachirps("C:\\Work\\Test\\XYpoints.csv", "C:\\Work\\Test\\Output\\dfNASAP.pkl", "C:\\Work\\Test\\Output\\dfCHIRPS.pkl", "C:\\Work\\Test\\Output")
#-------------------------------------------------------------------------------

import os
from osgeo import gdal, ogr
import joblib
import struct
import pandas as pd
import numpy as np
import more_itertools as mit
from datetime import datetime, timedelta

def qc(input_dir, out_dir):

    output_dir = out_dir + "\\DSSAT"

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    else:
        print("Directory " , output_dir ,  " already exists. Data will be overwritten")

    m_srad = []
    m_tmax = []
    m_tmin = []
    m_rain = []
    date_srad = []

    for wth_file in os.listdir(input_dir):
        if wth_file.endswith(".WTH"):
            with open(input_dir + "\\" + wth_file, "r") as f1:
                c_srad = 0
                c_tmax = 0
                c_tmin = 0
                c_rain = 0
                f_srad = 0
                solar = []
                i_srad = []
                CellID = os.path.splitext(wth_file)[0]

                data = f1.readlines()
                for index, field in enumerate(data[5:]):
                    DATE = field.split()[0]
                    SRAD = field.split()[1]
                    TMAX = field.split()[2]
                    TMIN = field.split()[3]
                    RAIN = field.split()[4]

                    solar.append(round(float(SRAD), 1))

                    if SRAD == "-99" or SRAD == "-99.0" or SRAD == "nan" or SRAD == "-3596.4":
                        c_srad += 1

                        date_srad.append(int(DATE))
                        i_srad.append(index)

                    if TMAX == "-99":
                        c_tmax += 1

                    if TMIN == "-99":
                        c_tmin += 1

                    if RAIN == "-99":
                        c_rain += 1

                tot_srad = len(date_srad)
                groups_srad = [list(group) for group in mit.consecutive_groups(date_srad)]
                lgroups_srad = [len(l) for l in groups_srad]
                freq_mv_srad = [[i,lgroups_srad.count(i)] for i in set(lgroups_srad)]

            m_srad.append(c_srad)
            m_tmax.append(c_tmax)
            m_tmin.append(c_tmin)
            m_rain.append(c_rain)

            with open(output_dir + "\\" + wth_file, "w") as f2:
                f2.writelines(data[0:5])

                for index2, field2 in enumerate(data[5:]):
                    DATE2 = field2.split()[0]
                    if index2 in i_srad:
                        if (((index2 + 1) and (index2 + 2)) in i_srad and ((index2 + 3)==len(data[5:]))):
                            SRAD2 = solar[index2 - 1]
                        elif ((index2 + 1) in i_srad and (index2 + 2)==len(data[5:])):
                            SRAD2 = solar[index2 - 2]
                        elif ((index2 - 2) in i_srad and (index2 - 1) in i_srad and (index2 + 1)==len(data[5:])):
                            SRAD2 = solar[index2 - 3]
                        elif (index2 + 1) in i_srad:
                            SRAD2 = round(solar[index2 - 1] + (solar[index2 + 2] - solar[index2 - 1])/3, 1)
                        elif (index2 - 1) in i_srad:
                            SRAD2 = round(solar[index2 - 2] + 2 * (solar[index2 + 1] - solar[index2 - 2])/3, 1)
                        elif (index2 + 1) == len(data[5:]):
                            SRAD2 = solar[index2 - 1]
                        else:
                            SRAD2 = round((solar[index2 - 1] + solar[index2 + 1])/2, 1)
                    else:
                      SRAD2 = round(float(field2.split()[1]), 1)

                    TMAX2 = field2.split()[2]
                    TMIN2 = field2.split()[3]
                    RAIN2 = field2.split()[4]
                    RHUM2 = field2.split()[5]
                    WIND2 = field2.split()[6]
                    TDEW2 = field2.split()[7]
                    #PAR2 = field2.split()[8]

                    f2.write('{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>5}'.format(DATE2, SRAD2, TMAX2, TMIN2,
                                                                                            RAIN2, RHUM2, WIND2, TDEW2))
                    f2.write("\n")

def nasaconv(df):

    # Conversion units [W.m-2] to [MJ.m-2.day-1].   SRAD
    df["ALLSKY_SFC_SW_DWN"] = df["ALLSKY_SFC_SW_DWN"] * 86400 / 1000000

    # Conversion units [K] to [C].   TMAX, TMIN, and TDEW
    df["T2M_MAX"] = df["T2M_MAX"] - 273.15
    df["T2M_MIN"] = df["T2M_MIN"] - 273.15
    df["T2MDEW"] = df["T2MDEW"] - 273.15

    # Conversion units [kg.m-2.s-1] to [mm].   RAIN
    df["PRECTOTCORR"] = 86400 * df["PRECTOTCORR"]

    # Conversion units [m.s-1] to [km.day-1].   WIND
    df["WS2M"] = 86.4 * df["WS2M"]

    nasaptime = []
    for i in df["time"]:
        sdate = datetime.fromtimestamp(datetime.timestamp(i)).date().strftime('%y%j')
        nasaptime.append(sdate)

    df.insert(loc=4, column='time2', value=nasaptime)

def nasachirps(in_file, nasap_file, chirps_file, out_dir, NASAP_ID = "nasapid", ID = "ID"):

    out_file = out_dir + "\\DSSAT0"
    if not os.path.exists(out_file):
        os.mkdir(out_file)
    else:
        print("Directory ", out_file, " already exists. Data will be overwritten")

    df1 = joblib.load(chirps_file)
    d = df1.columns.values.tolist()
    df2 = joblib.load(nasap_file)

    nasaconv(df2)

    hdr1 = "*WEATHER DATA\n\n"
    hdr2 = '{:>6} {:>10} {:>11} {:>10} {:>5} {:>5} {:>5} {:>5}'.format("@ INSI", "LAT", "LONG", "ELEV", "TAV", "AMP", "REFHT", "WNDHT" + "\n")
    hdr4 = '{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>6}'.format("@DATE", "SRAD", "TMAX", "TMIN", "RAIN", "RHUM", "WIND", "TDEW" + "\n")

    pt = pd.read_csv(in_file)

    for ind in pt.index:
        input_id = pt[ID][ind]
        npid = pt[NASAP_ID][ind]
        LAT = round(pt['Latitude'][ind], 5)
        LONG = round(pt['Longitude'][ind], 5)

        NASAPval = df2.loc[(df2[ID] == npid)]

        tavg1 = (NASAPval['T2M_MIN'] + NASAPval['T2M_MAX']) / 2
        NASAPval.insert(loc=12, column='tavg', value=tavg1)

        dfmonthly = NASAPval.resample('M', on='time').mean()
        TAV = round(dfmonthly["tavg"].mean(), 1)
        AMP = round(dfmonthly["tavg"].max() - dfmonthly["tavg"].min(), 1)

        with open(out_file + "\\" + str(input_id) + ".WTH", "w") as f1:

            f1.write(hdr1)
            f1.write(hdr2)
            f1.write('{:>6} {:>10} {:>11} {:>10} {:>5} {:>5} {:>5} {:>5}'.format("UFL", LAT, LONG, "", TAV, AMP, "2.0",
                                                                              "2.0") + "\n")
            f1.write(hdr4)

            for index, row in NASAPval.iterrows():
                DATE = row['time2']
                SRAD = round(row['ALLSKY_SFC_SW_DWN'], 1)
                TMAX = round(row['T2M_MAX'], 1)
                TMIN = round(row['T2M_MIN'], 1)
                RHUM = round(row['RH2M'], 1)
                WIND = round(row['WS2M'], 1)
                TDEW = round(row['T2MDEW'], 1)

                dat = datetime.fromtimestamp(datetime.timestamp(row['time'])).date().strftime('%Y%m%d')

                if dat in d:
                    RAIN_CHIRPS = round(df1.loc[input_id, dat], 1)
                    if RAIN_CHIRPS == -9999.0:
                        RAIN = round(row['PRECTOTCORR'], 1)
                    else:
                        RAIN = RAIN_CHIRPS
                else:
                    RAIN = round(row['PRECTOTCORR'], 1)

                f1.write('{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>5}'.format(DATE, SRAD, TMAX, TMIN, RAIN,
                                                                                       RHUM, WIND, TDEW))
                f1.write("\n")

    qc(out_file, out_dir)
