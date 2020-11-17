#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Name:         CHIRPS.py
# Purpose:      To fetch precipitation data from TIFF files provided by the CHIRPS project.
# Author:       Oscar Castillo
#               ocastilloromero@ufl.edu
# Created:     06/04/2020
# Runs in Python 3.8.5
# Example:
# chirps(in_chirps, in_file, out_dir, ID = "ID")
# chirps("C:\\Work\\Test\\in_chirps", "C:\\Work\\Test\\XYpoints.csv", "C:\\Work\\Test\\Output")
#-------------------------------------------------------------------------------

import os
from osgeo import gdal, ogr, osr
import joblib
import struct
import pandas as pd
import more_itertools as mit
import datetime

def csv_shp(in_file, out_dir):

    out_shp = out_dir + "\\XY_Points.shp"
    driver = ogr.GetDriverByName("ESRI Shapefile")

    if os.path.exists(out_shp):
        print(out_shp, " already exists. It will be overwritten.")
        driver.DeleteDataSource(out_shp)

    data_source = driver.CreateDataSource(out_shp)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = data_source.CreateLayer(os.path.splitext(out_shp)[0], srs, ogr.wkbPoint)

    layer.CreateField(ogr.FieldDefn("ID", ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn("Latitude", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("Longitude", ogr.OFTReal))
    layer.CreateField(ogr.FieldDefn("nasapid", ogr.OFTInteger))
    #layer.CreateField(ogr.FieldDefn("LatNP", ogr.OFTReal))
    #layer.CreateField(ogr.FieldDefn("LonNP", ogr.OFTReal))

    with open(in_file, "r") as f1:
        data = [line for line in f1.readlines() if line.strip()]

        for row in data[1:]:
          feature = ogr.Feature(layer.GetLayerDefn())
          feature.SetField("ID", row.split(',')[0])
          feature.SetField("Latitude", row.split(',')[1])
          feature.SetField("Longitude", row.split(',')[2])
          feature.SetField("nasapid", row.split(',')[3])
          #feature.SetField("LatNP", row.split(',')[4])
          #feature.SetField("LonNP", row.split(',')[5])

          wkt = "POINT(%f %f)" %  (float(row.split(',')[2]) , float(row.split(',')[1]))
          point = ogr.CreateGeometryFromWkt(wkt)
          feature.SetGeometry(point)
          layer.CreateFeature(feature)
          feature = None

    data_source = None

def chirps(in_chirps, in_file, out_dir, ID = "ID"):

    df_out = out_dir + "\\dfCHIRPS.pkl"

    if os.path.isfile(df_out):
        print("The", df_out, "file already exists. It will be used.")
        return

    csv_shp(in_file, out_dir)
    shp = out_dir + "\\XY_Points.shp"

    ds=ogr.Open(shp)
    lyr=ds.GetLayer()

    CellID = []

    for feat in lyr:
        Id = feat.GetField(ID)
        CellID.append(Id)

    df = pd.DataFrame()
    df[ID] = CellID
    df_chirps = df.set_index(ID)

    for chirps_file in os.listdir(in_chirps):
        if chirps_file.endswith(".tif"):
            chirps_fname = chirps_file[-14:-4].replace(".", "")
            img1_ds = gdal.Open(in_chirps + "\\" + chirps_file)
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
                if val is None:
                    z = -9999.0
                    pixelv.append(z)
                else:
                    z = val[0].tolist()
                    pixelv.append(z[0])

        df_chirps[chirps_fname] = pixelv

    joblib.dump(df_chirps, df_out)
    print("The file", df_out, "was successfully created.")

    return df_chirps

