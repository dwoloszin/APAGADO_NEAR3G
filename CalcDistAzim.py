import pyproj
from operator import itemgetter
import timeit


#Dario Woloszin

def CalcDist(latA,lonA,latB,lonB):
    latitudeA = str(latA).replace(',','.')
    longitudeA = str(lonA).replace(',','.')

    latitudeB = str(latB).replace(',','.')
    longitudeB = str(lonB).replace(',','.')

    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitudeA, latitudeA, longitudeB, latitudeB)
    #dist = str(round(distance,2)/1000).replace('.',',')
    dist = round(distance,2)
    return dist

def CalcDist2(latA,lonA,latB,lonB):
    latitudeA = str(latA).replace(',','.')
    longitudeA = str(lonA).replace(',','.')

    latitudeB = str(latB).replace(',','.')
    longitudeB = str(lonB).replace(',','.')

    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitudeA, latitudeA, longitudeB, latitudeB)
    dist = str(round(distance,2)/1000).replace('.',',')
    return dist
