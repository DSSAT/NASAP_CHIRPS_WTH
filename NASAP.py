#-------------------------------------------------------------------------------
# Name:        NASAP
# Purpose:     To fetch point weather data from NASA POWER server.
#
# Author:      Oscar Castillo
#              ocastilloromero@ufl.edu
# Created:     19/02/2019
# Copyright:   (c) ocastilloromero 2019
# Licence:     University of Florida
#Runs in Python 2.7
##Example
#nasa("E:\\Test\\Points.txt", "20190601", "20190630")
#-------------------------------------------------------------------------------
from threading import Thread
import httplib
import os, sys
from Queue import Queue
import requests
from requests.adapters import HTTPAdapter
import simplejson
import urllib2
import traceback

def nasa(user_input, startDate, endDate):
    output_dir = os.path.dirname(user_input) + "\\NASAP"

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    else:
        print("Directory " , output_dir ,  " already exists. Data will be overwritten")

    concurrent = 10

    def doWork():
        while True:
            url = q.get()
            txt = getTxt(url[0], url[1])
            q.task_done()

    def getTxt(ourl, Cell):
        try:
            s = requests.Session()
            s.mount("https://power.larc.nasa.gov", HTTPAdapter(max_retries=10))

            while True:
                response = s.get(ourl, timeout=None)
                if response.status_code == requests.codes.ok:
                    if response.text == "":
                        response = s.get(ourl, timeout=None)
                        continue
                    else:
                        js = simplejson.loads(response.text)
                        if js["messages"] == []:
                            js = simplejson.loads(response.text)
                            break
                        else:
                            continue

            txt_URL = js["outputs"]["icasa"]

            filedata = urllib2.urlopen(txt_URL).read()

            file = open( output_dir + "\\" + Cell + ".txt", "w")

            file.write(filedata)
            file.close()

        except:
            e = sys.exc_info()[1]
            print(e.args[0], "ID: ", Cell)

            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])

    q = Queue(concurrent * 2)

    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()

    in_file = open(user_input)

    iterfile = iter(in_file)
    next(iterfile)

    try:
        for coord in iterfile:
            CellID = coord.split()[0]
            Lat = coord.split()[1]
            Lon = coord.split()[2]
            url = ["https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py?&request=execute&identifier=SinglePoint&parameters=PRECTOT,T2M_MAX,T2M_MIN,ALLSKY_SFC_SW_DWN&userCommunity=AG&tempAverage=DAILY&outputList=ICASA&startDate=" + startDate + "&endDate=" + endDate + "&lat=" + Lat + "&lon=" + Lon, CellID]
            q.put(url)
        q.join()

    except KeyboardInterrupt:
        sys.exit(1)
