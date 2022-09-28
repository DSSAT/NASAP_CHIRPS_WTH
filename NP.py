#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:         NP.py
# Purpose:      To get the NASAPOWER weather variables using the OPeNDAP server.
# Author:       Oscar Castillo
#               ocastilloromero@ufl.edu
#               Range_Days, DataRequest, and nasap_point functions were adapted
#               from the original version provided by the NASAPOWER developers team.
# Created:     06/04/2020
# Updates:     08/13/2021 TF - Updated NASAP request function to work using NASAP API
# Runs in Python 3.8.5
# Example:
#Columns of the CSV file:
#ID,Latitude,Longitude, nasapid, LatNP, LonNP
#nasap_gen(in_file, out_dir, sy, sm, sd, ey, em, ed, NASAP_ID = "nasapid")
#nasap_gen("C:/Work/Test/XYpoints.csv", "C:/Work/Test/Output", 2020, 10, 29, 2020, 10, 31)
#-------------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import os, netCDF4
import pandas as pd
import xarray as xr
import numpy as np
import joblib
import requests
import urllib.request

#def Range_Days(Start_Date, End_Date):
#    Days = []
#    for each in range((End_Date - Start_Date).days + 1):
#        Days.append(Start_Date + timedelta(days=each))
#    return Days

#def DataRequest(Collection):
#    URL, Parameter, Latitude, Longitude, Times = Collection
#    df = xr.open_dataset(URL).sel(lon=Longitude, lat=Latitude, time=Times, method='nearest')[Parameter]
#    return df

# 08/13/2021 TF - Updated nasap_point function to request data using NASAP API
def nasap_point(ycoord, xcoord, sy, sm, sd, ey, em, ed, rformat):
    headers = {
        'accept': 'application/json',
    }

    params = (
        ('start', str(sy)+str(sm)+str(sd)),
        ('end', str(ey)+str(em)+str(ed)),
        ('latitude', str(ycoord)),
        ('longitude', str(xcoord)),
        ('community', 'ag'),
        ('parameters', 'T2MDEW,T2M_MIN,T2M_MAX,RH2M,PRECTOTCORR,WS2M,ALLSKY_SFC_SW_DWN'),
        ('format', rformat),
        ('header', 'true'),
        ('time-standard', 'lst'),
    )
    #print("Submitting request ...")
    response = requests.get('https://power.larc.nasa.gov/api/temporal/daily/point', headers=headers, params=params)
    #print("Recieved API response ...")
    
    if(rformat =='netcdf'):
        nc4_ds = netCDF4.Dataset('daily_nasap', memory=response.content)
        store = xr.backends.NetCDF4DataStore(nc4_ds)
        df = xr.open_dataset(store)

        df1 = df.to_dataframe()
        df2 = df1.reset_index()
        ylat = df2.loc[0,"lat"]
        xlon = df2.loc[0,"lon"]
    elif(rformat=='json'):
        df = response.json()
        ylat = df['geometry']['coordinates'][1]
        xlon = df['geometry']['coordinates'][0]
        df1 = df['properties']['parameter']
        df2 = pd.DataFrame.from_dict(df1, orient='columns')
        df2['time'] = df2.index
        df2['time'] = pd.to_datetime(df2['time'])
    else:
        print("The format", rformat, "is not available. Please inform a valid data format (json or netcdf).")
        exit()

    return df2, ylat, xlon

def nasap_gen(in_file, out_dir, sy, sm, sd, ey, em, ed, rformat, NASAP_ID = "nasapid"):

    ndays = (datetime(ey, em, ed) - datetime(sy, sm, sd)).days + 1
    df_out = out_dir + "/dfNASAP.pkl"

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
        df, y, x = nasap_point(lat, lon, sy, sm, sd, ey, em, ed, rformat)
        id_vec = [Id] * ndays
        df.insert(loc=0, column=NASAP_ID, value=id_vec)
        df.to_csv(out_dir + "/" + "{}.txt".format(Id), index=False, sep='\t')
    else:
        Id = pt.loc[:, NASAP_ID]
        lat = pt.loc[:,"LatNP"]
        lon = pt.loc[:,"LonNP"]

        for n, i, j in zip(Id, lat, lon):
            df, y, x = nasap_point(i, j, sy, sm, sd, ey, em, ed, rformat)
            id_vec = [n] * ndays
            df.insert(loc=0, column='ID', value=id_vec)
            df1.append(df)
            print(n)

        result = pd.concat(df1)
        joblib.dump(result, df_out)
        print("The", df_out, "file was successfully created.")

