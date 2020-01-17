#-------------------------------------------------------------------------------
# Name:        DSSAT
# Purpose:     To create weather files in DSSAT format
#
# Author:      Oscar Castillo
#              ocastilloromero@ufl.edu
# Created:     17/01/2020
# Copyright:   (c) ocastilloromero 2020
# Licence:     University of Florida
# Runs in Python 3.7
# Example:
# nasachirps("E:\\Test\\NASAP", "E:\\Test\\CHIRPS_in", "E:\\Test\\XYPoints.shp")
#-------------------------------------------------------------------------------

import os
from osgeo import gdal, ogr
import joblib
import struct
import pandas as pd
import more_itertools as mit

def chirps(in_dir, shp, ID_name):

    ds=ogr.Open(shp)
    lyr=ds.GetLayer()

    CellID = []

    for feat in lyr:
        ID = feat.GetField(ID_name)
        CellID.append(ID)

    df = pd.DataFrame()
    df[ID_name] = CellID
    df_chirps = df.set_index(ID_name)

    for chirps_file in os.listdir(in_dir):
        if chirps_file.endswith(".tif"):
            chirps_fname = chirps_file[-14:-4].replace(".", "")
            img1_ds = gdal.Open(in_dir + "\\" + chirps_file)
            gt = img1_ds.GetGeoTransform()
            rb = img1_ds.GetRasterBand(1)

            ds=ogr.Open(shp)
            lyr=ds.GetLayer()

            pixelv = []

            for feat in lyr:
                pt = feat.geometry()
                x = pt.GetX()
                y = pt.GetY()

                geom = feat.GetGeometryRef()
                mx,my = geom.GetX(), geom.GetY()

                px = int((mx - gt[0]) / gt[1])
                py = int((my - gt[3]) / gt[5])

                val=rb.ReadAsArray(px,py,1,1)
                z = val[0].tolist()
                pixelv.append(z[0])

        df_chirps[chirps_fname] = pixelv

    return df_chirps

def qc(input_dir):

    os.chdir(input_dir)
    output_dir = os.path.dirname(input_dir) + "\\DSSAT"

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
            with open(wth_file, "r") as f1:
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

                    if SRAD == "-99" or SRAD == "-99.0":
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
                    PAR2 = field2.split()[8]

                    f2.write('{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>4}'.format(DATE2, SRAD2, TMAX2, TMIN2, RAIN2, RHUM2, WIND2, TDEW2, PAR2))
                    f2.write("\n")


def nasachirps(in_fileNP, in_dir, shp, NASAP_ID = "ID", ID = "ID"):

    out_file = os.path.dirname(in_fileNP) + "\\DSSAT0"

    if not os.path.exists(out_file):
        os.mkdir(out_file)
    else:
        print("Directory " , out_file ,  " already exists. Data will be overwritten")

    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shp, 0)
    layer = dataSource.GetLayer()
    hdr1 = "*WEATHER DATA\n\n"

    df = chirps(in_dir, shp, "ID")

    for feature in layer:
        npid = feature.GetField(NASAP_ID)
        input_id = feature.GetField(ID)
        with open(in_fileNP + "\\" + str(npid) + ".txt", "r") as f1:
            with open(out_file + "\\" + str(feature.GetField(ID)) + ".WTH", "w") as f2:
                f2.write(hdr1)
                f2.write('{:>6} {:>8} {:>8} {:>7} {:>5} {:>5} {:>5} {:>5}'.format("@ INSI", "LAT", "LONG", "ELEV", "TAV", "AMP", "REFHT", "WNDHT" + "\n"))

                data = [line for line in f1.readlines() if line.strip()]

                LAT = data[11].split()[1]
                LONG = data[11].split()[2]
                ELEV = data[11].split()[3]
                WNDHT = data[11].split()[5]

                f2.write('{:>6} {:>8} {:>8} {:>7} {:>5} {:>5} {:>5} {:>6}'.format("UFL", LAT, LONG, ELEV, "", "", "", WNDHT + "\n"))
                f2.write('{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>5} {:>5}'.format("@DATE", "SRAD", "TMAX", "TMIN", "RAIN", "RHUM", "WIND", "TDEW", "PAR" + "\n"))

                values = df.loc[input_id, :].values.tolist()

                for index, field in enumerate(data[13:]):
                    DATE = field.split()[0]
                    SRAD = round(float(field.split()[8]), 1)
                    TMAX = round(float(field.split()[1]), 1)
                    TMIN = round(float(field.split()[4]), 1)

                    RAIN_CHIRPS = round(values[index], 1)
                    if RAIN_CHIRPS == -9999.0:
                        RAIN = round(float(field.split()[5]), 1)
                    else:
                        RAIN = RAIN_CHIRPS

                    RHUM = round(float(field.split()[2]), 1)
                    WIND = round(float(field.split()[7])*86.4, 1)
                    TDEW = round(float(field.split()[6]), 1)
                    PAR = -99

                    my_list = [DATE, SRAD, TMAX, TMIN, RAIN, RHUM, WIND, TDEW, PAR]
                    f2.write('{:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} {:>5} {:>4}'.format(DATE, SRAD, TMAX, TMIN, RAIN, RHUM, WIND, TDEW, PAR))
                    f2.write("\n")

    layer.ResetReading()

    qc(out_file)

