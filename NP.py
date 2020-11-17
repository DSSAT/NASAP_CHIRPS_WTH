#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:         NP.py
# Purpose:      To get the NASAPOWER weather variables using the OPeNDAP server.
# Author:       Oscar Castillo
#               ocastilloromero@ufl.edu
#               Range_Days, DataRequest, and nasap_point functions were adapted
#               from the original version provided by the NASAPOWER developers team.
# Created:     06/04/2020
# Runs in Python 3.8.5
# Example:
#Columns of the CSV file:
#ID,Latitude,Longitude, nasapid, LatNP, LonNP
#nasap_gen(in_file, out_dir, sy, sm, sd, ey, em, ed, NASAP_ID = "nasapid")
#nasap_gen("C:\\Work\\Test\\XYpoints.csv", "C:\\Work\\Test\\Output", 2020, 10, 29, 2020, 10, 31)
#-------------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import os, netCDF4
import pandas as pd
import xarray as xr
import numpy as np
import joblib
from datetime import datetime
import urllib.request

def Range_Days(Start_Date, End_Date):
    Days = []
    for each in range((End_Date - Start_Date).days + 1):
        Days.append(Start_Date + timedelta(days=each))
    return Days

def DataRequest(Collection):
    URL, Parameter, Latitude, Longitude, Times = Collection
    df = xr.open_dataset(URL).sel(lon=Longitude, lat=Latitude, time=Times, method='nearest')[Parameter]
    return df

def nasap_point(ycoord, xcoord, sy, sm, sd, ey, em, ed):
    List = [
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_t2mdew_lst.nc", "T2MDEW"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_t2m_lst.nc", "T2M_MIN"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_t2m_lst.nc", "T2M_MAX"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_rh2m_lst.nc", "RH2M"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_prectotcorr_lst.nc", "PRECTOTCORR"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_ws2m_lst.nc", "WS2M"),
        ("https://opendap.larc.nasa.gov/opendap/hyrax/POWER/daily/power_801_daily_allsky_sfc_sw_dwn_lst.nc", "ALLSKY_SFC_SW_DWN"),
    ]

    Latitude = np.array([ycoord])
    Longitude = np.array([xcoord])
    Times = Range_Days(datetime(sy, sm, sd), datetime(ey, em, ed))
    pool = ThreadPoolExecutor(None)

    Futures = []
    for URL, Parameter in List:
        Collection = (URL, Parameter, Latitude, Longitude, Times)
        Futures.append(pool.submit(DataRequest, Collection))

    wait(Futures);
    df = xr.merge([Future.result() for Future in Futures])
    df1 = df.to_dataframe()
    df2 = df1.reset_index()
    ylat = df2.loc[0,"lat"]
    xlon = df2.loc[0,"lon"]

    return df2, ylat, xlon

def nasap_gen(in_file, out_dir, sy, sm, sd, ey, em, ed, NASAP_ID = "nasapid"):

    ndays = (datetime(ey, em, ed) - datetime(sy, sm, sd)).days + 1
    df_out = out_dir + "\\dfNASAP.pkl"

    if os.path.isfile(df_out):
        print("The", df_out, "file already exists. This file will be used.")
        return

    pt = pd.read_csv(in_file)
    pt = pt.drop_duplicates(subset=[NASAP_ID])
    df1 =[]

    if pt.ndim == 1:
        Id = pt.loc[NASAP_ID].astype(int)
        lat = pt.loc["LatNP"]
        lon = pt.loc["LonNP"]
        df, y, x = nasap_point(lat, lon, sy, sm, sd, ey, em, ed)
        id_vec = [Id] * ndays
        df.insert(loc=0, column=NASAP_ID, value=id_vec)
        df.to_csv(out_dir + "\\" + "{}.txt".format(Id), index=False, sep='\t')
    else:
        Id = pt.loc[:, NASAP_ID]
        lat = pt.loc[:,"LatNP"]
        lon = pt.loc[:,"LonNP"]

        for n, i, j in zip(Id, lat, lon):
            df, y, x = nasap_point(i, j, sy, sm, sd, ey, em, ed)
            id_vec = [n] * ndays
            df.insert(loc=0, column='ID', value=id_vec)
            df1.append(df)
            print(n)

        result = pd.concat(df1)
        joblib.dump(result, df_out)
        print("The", df_out, "file was successfully created.")

