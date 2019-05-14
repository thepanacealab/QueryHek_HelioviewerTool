# @author Bhaskar Ray
# @version 11/26/2018

import urllib.request
import urllib.parse
import csv
import json as j
import pandas as pd
import datetime
from pgmagick import Image
import cv2
import os
import numpy as np
import time



def helioviewer():
    CDELT = float(0.599733)
    HPCCENTER = float(4096.0 / 2.0)
    # inputCsvDataFile = input("Enter input csv data file directory path with csv data file name: ")
    inputCsvDataFile = "AR_SPoCA_2015-01-01_2015-12-31.csv"
    dataNew = pd.read_csv(inputCsvDataFile, usecols=['event_starttime', 'event_endtime', 'hpc_bbox'])

    event_startTime = dataNew.event_starttime.tolist()
    event_endTime = dataNew.event_endtime.tolist()
    hpc_bboxPolygon = dataNew.hpc_bbox.tolist()
    # directoryJp2 = input("Enter Jp2 image output directory path: ")
    directoryJp2 = "jp2"
    if not os.path.isdir(directoryJp2):
        os.makedirs(directoryJp2)
    counter = 0
    for event_startTimeData in event_startTime:

        event_endTimeDate = event_endTime[counter]
        hpc_bboxPolygonData = hpc_bboxPolygon[counter]
        Coordinates = hpc_bboxPolygonData.replace('POLYGON((', '?').replace(" ", '?').replace(",", '?').replace(
            "))", '?').split('?')
        x1 = round(float(HPCCENTER + (float(Coordinates[1]) / CDELT)))
        y1 = round(float(HPCCENTER - (float(Coordinates[2]) / CDELT)))
        x2 = round(float(HPCCENTER + (float(Coordinates[3]) / CDELT)))
        y2 = round(float(HPCCENTER - (float(Coordinates[4]) / CDELT)))
        x3 = round(float(HPCCENTER + (float(Coordinates[5]) / CDELT)))
        y3 = round(float(HPCCENTER - (float(Coordinates[6]) / CDELT)))
        x4 = round(float(HPCCENTER + (float(Coordinates[7]) / CDELT)))
        y4 = round(float(HPCCENTER - (float(Coordinates[8]) / CDELT)))
        event_startTimeDataFolder = event_startTimeData
        event_endTimeDateFolder = event_endTimeDate
        directoryJp2TrackID = directoryJp2 + "\\" + "_" + event_startTimeDataFolder.replace(":",
                                                                                            "_") + "_" + event_endTimeDateFolder.replace(
            ":", "_") + "_Count" + str(counter)
        print(directoryJp2TrackID)
        if not os.path.isdir(directoryJp2TrackID):
            os.makedirs(directoryJp2TrackID)
        counter = counter + 1
        if counter%5==0:
            num=counter*400
            print('Delay after:', str(num))
            time.sleep(5)
        start = datetime.datetime.strptime(event_startTimeData, "%Y-%m-%dT%H:%M:%S")
        end = datetime.datetime.strptime(event_endTimeDate, "%Y-%m-%dT%H:%M:%S")
        while start <= end:

            try:

                    dateDownload = str(start.date()) + "T" + str(start.time()) + "Z"
                    print(dateDownload)
                    uriHelioviewer = "https://api.helioviewer.org/v2/getJP2Image/?"
                    valuesImageUri = {"date": dateDownload, "sourceId": "11",
                                      "jpip": "true"}
                    dataImageUri = urllib.parse.urlencode(valuesImageUri)
                    reqImageUri = urllib.request.Request(uriHelioviewer + dataImageUri)
                    respImageUri = urllib.request.urlopen(reqImageUri)
                    resImageUriData = respImageUri.read()
                    resImageUriData = str(resImageUriData).split("193", 1)[1]
                    resImageUriData = resImageUriData.replace(".jp2'", "")
                    resImageUriData = resImageUriData.strip()
                    print(resImageUriData)
                    urlHelioviewer = "https://api.helioviewer.org/v2/getJP2Image/?"
                    valuesImage = {"date": dateDownload, "sourceId": "11"}
                    dataImage = urllib.parse.urlencode(valuesImage)
                    urllib.request.urlretrieve(urlHelioviewer + dataImage,
                                               directoryJp2TrackID + resImageUriData + ".jp2")
                    start = start + datetime.timedelta(seconds=36)
                    print(start)

            except Exception as e:
                    print('Error:', str(e))
                    time.sleep(600)
                    continue




def main():
    helioviewer()


if __name__ == "__main__":
    main()
