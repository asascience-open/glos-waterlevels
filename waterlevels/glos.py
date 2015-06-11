#!/usr/bin/env python

#Created by Dan Maher, RPS ASA, 3-7-2014

import requests
import time as clock

from datetime import datetime
from datetime import date
from datetime import timedelta
import os
import sys
import argparse
import re

import numpy as np
import zipfile
import StringIO
import logging
import os
import pandas
import netCDF4

from waterlevels.config import GLWL_URL, CACHE_DIR, DATA_DIR, WATER_LEVEL_UNITS

def data_download ():
    filename = download_file(GLWL_URL, CACHE_DIR)
    return filename


def download_file(url, path):
    local_filename = url.split('/')[-1]
    local_filename = os.path.join(path, local_filename)
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

#--------------------------------------------------------------------------------
def getLatLon(station_list):
    pos_dict = {
                'Toronto':
                (43.63, -79.38),
                'Rochester':
                (43.27, -77.62),
                'Port Weller':
                (43.23, -79.22),
                'Oswego':
                (43.45, -76.50),
                'Kingston':
                (44.22, -76.52),
                'Cobourg':
                (43.95, -78.17),

                'St Clair Shores':
                (42.47, -82.87),
                'Belle River':
                (42.30, -82.72),

                'Port Stanley':
                (42.67, -81.22),
                'Toledo':
                (41.68, -83.47),
                'Cleveland':
                (41.53, -81.63),
                'Port Colborne':
                (42.87, -79.25),


                'Thunder Bay':
                (48.40, -89.22),
                'Michi- picoten':
                (47.97, -84.90),
                'SW Pier':
                (46.50, -84.37),
                'Duluth':
                (46.77, -92.08),
                'Pt Iroquois':
                (46.48, -84.62),
                'Marquette':
                (46.53, -87.27),
                'US Slip':
                (46.50, -84.34),

                'Mackinaw City':
                (45.77, -84.72),
                'Tobermory':
                (45.25, -81.67),
                'Ludington':
                (43.93, -86.43),
                'Thessalon':
                (46.25, -83.55),
                'Milwaukee':
                (43.00, -87.88),
                'Harbor Beach':
                (43.83, -82.63),

                'Ontario Adj. Daily Mean':
                (43.70, -77.90),
                'Superior Daily Mean':
                (47.70, -87.40),
                'Michigan Daily Mean':
                (44.00, -87.00),
                'St. Clair Daily Mean':
                (42.47, -82.67),
                'Erie Daily Mean':
                (42.20, -81.20),
                'Huron Daily Mean':
                (44.80,-82.40),



                'Ontario Adj. Lake Mean':
                (43.70, -77.90),
                'Ontario Lake Mean':
                (43.70, -77.90),
                'Superior Lake Mean':
                (47.70, -87.40),
                'Michigan Huron Lake Mean':
                (44.00, -87.00),
                'St Clair Lake Mean':
                (42.47, -82.67),
                'Erie Lake Mean':
                (42.20, -81.20),
                'Michigan Huron Lake Mean':
                (44.80,-82.40)
    }

    pos_list = []
    for each in station_list:
        pos_list.append(pos_dict[each.strip()])

    lat_list = []
    lon_list = []
    for lat,lon in pos_list:
        lat_list.append(lat)
        lon_list.append(lon)
    return lat_list, lon_list

#--------------------------------------------------------------------------------
def getTime(page):
    time_list_start = page['Date'].values
    time_list = []
    for each in time_list_start:
        time_list.append(int(clock.mktime(clock.strptime(each,'%d-%b-%Y'))))

    return time_list, time_list_start
#--------------------------------------------------------------------------------
def pageParse(pageno, report_path):
    assert os.path.exists(report_path)
    opts = {
        'pageno' : pageno,
        'report_path' : report_path,
        'page_path' : os.path.join(CACHE_DIR, 'page%s.txt' % pageno)
    }

    command = 'pdftotext -layout -f %(pageno)s -l %(pageno)s %(report_path)s %(page_path)s' % opts
    print command
    os.system(command)
    filename = opts['page_path']
    # Using the newer with construct to close the file automatically.
    with open(filename) as f:
        read_data = f.readlines()



    title_index = [i for i, s in enumerate(read_data) if re.search('Water Levels$',s)]
    title = ''.join(read_data[title_index[0]].strip())
    title = title.replace(' ', '')

    indices = [i for i, s in enumerate(read_data) if re.search('\d\d-[\w]*-\d\d\d\d',s)]

    header_indices = []
    for each in xrange(indices[0]-1,0,-1):
        if read_data[each].strip() == '' and each != indices[0]-1:
            header_indices.append(each+1)
            if read_data[indices[0]-1].strip() != '':
                header_indices.append(indices[0])
            else:
                header_indices.append(indices[0]-1)
            break



    with open(opts['page_path'],'w') as pagetable:

        for line in read_data[header_indices[0]:header_indices[1]]:
            pagetable.write(line)
        for index in indices:
            pagetable.write(read_data[index])

        header = range(header_indices[1]-header_indices[0])
    page =  pandas.read_fwf(opts['page_path'], sep=r'\s*', header=header, tupleize_cols=True )

    for each in page.keys():
        new_name = []
        for every in each:
            if 'Unnamed' not in every:
                new_name.append(str(every))
        page[' '.join(new_name).replace('*','')] = page.pop(each)



    return page, title
#--------------------------------------------------------------------------------

def dataCreate(page):
    data = []
    try:
        page['Huron Daily Mean'] = page['Michigan Huron Daily Mean']
        page['Michigan Daily Mean'] = page.pop('Michigan Huron Daily Mean')
    except:
        pass

    for key in page.keys():
        if key != 'Date':
            data.append(page[key].values)

    return data, page
#--------------------------------------------------------------------------------
def stationCreate(page):
    station_list = []
    for each in page.keys():
        if each != 'Date':
            station_list.append(each.ljust(25))
    return station_list
#--------------------------------------------------------------------------------
def ncCreate(pageno, report_path):

    page_temp, title = pageParse(pageno, report_path)
    data, page = dataCreate(page_temp)
    station_list = stationCreate(page)
    #Set some defualy parameters
    global_attributes = []
    attributes = []
    fillvalue = -999.9
    _fillValue = -999.9

    time_list, time_list_start = getTime(page)
    month = time_list_start[0][3:]
    date_time_list = [datetime.fromtimestamp(i) for i in time_list]
    dtg = date_time_list[0].strftime('%Y-%m')
    #Define the location and name for saving the file

    output_directory = os.path.join(DATA_DIR, title)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_filename = "GreatLakesWaterLevels_%s_%s.nc" % (title,dtg)
    filepath = os.path.join(output_directory, output_filename)
    print filepath
    lat_list,lon_list = getLatLon(station_list)
    if len(lat_list) != len(station_list):
        print 'Bad Dataset (PDF never populated with data).  No File Written.'
        return

    nc = netCDF4.Dataset(filepath, "w")
    nc.createDimension("station", len(station_list))
    nc.createDimension("station_name", len(station_list[0]))

    station_name = nc.createVariable("timeSeries", "c", ("station" ,"station_name"))
    station_name.cf_role = "timeSeries_id"
    station_name.long_name = 'station_name'
    station_name.standard_name = "platform_id"
    station_name[:] = station_list

    lat = nc.createVariable("lat", "f4", ('station'))
    lat.units           = "degrees_north"
    lat.standard_name   = "latitude"
    lat.long_name       = "station latitude"
    lat.axis = "Y"
    lat[:] = lat_list
    lon = nc.createVariable("lon", "f4", ('station'))
    lon.units           = "degrees_east"
    lon.standard_name   = "longitude"
    lon.long_name       = "station longitude"
    lon.axis = "X"
    lon[:] = lon_list

    nc.createDimension("time", len(time_list))
    times = nc.createVariable("time",    "f4", ( "time"))
    times.units          = "seconds"
    times.standard_name  = "time"
    times.long_name      = "time of measurement"
    times.calendar       = "gregorian"
    times._CoordianteAxisType = "time"
    times.units = "seconds since 1970-01-01 00:00:00"
    times[:] = time_list

    nc.setncattr("time_coverage_start",    (time_list[0]))
    nc.setncattr("time_coverage_end",     (time_list[-1]))
    #nc.setncattr("time_coverage_duration", time_list[0]-time_list[-1])
    nc.setncattr("creator_email", "hhpm@usace.army.mil")
    nc.setncattr("creator_url", "http://www.lre.usace.army.mil/Missions/GreatLakesInformation/GreatLakesWaterLevels.aspx")
    nc.setncattr("acknowledgement", 'Daily mean Great Lakes water levels are harvested from the Great Lakes Water Level reports (link) on the Great Lakes Information pages (link) maintained by the Detroit District of the US Army Corps of Engineers (link). The mean water levels for each of the Great Lakes is calculated from individual water level sensors according to guidance provided by the Coordinating Committee on Great Lakes Basic Hydraulic and Hydrologic Data.')
    nc.setncattr('institution', 'US Army Corps of Engineers')
    nc.setncattr('comment', 'These data are harvested with permission from the U.S. Army Corps of Engineers\' Great Lakes Water Levels Reports. (link to http://www.lre.usace.army.mil/Missions/GreatLakesInformation/GreatLakesWaterLevels/CurrentConditions.aspx)')

    nc.setncattr("time_coverage_resolution", "0")
    nc.setncattr("Conventions", "CF-1.6")
    #nc.setncattr("date_created", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z"))
    #Metadata variables
    crs = nc.createVariable("crs", "i4")
    crs.long_name           = "http://www.opengis.net/def/crs/EPSG/0/4326"
    crs.grid_mapping_name   = "latitude_longitude"
    crs.epsg_code           = "EPSG:4326"
    crs.semi_major_axis     = float(6378137.0)
    crs.inverse_flattening  = float(298.257223563)

    nc.project = 'GLOS'
    nc.title = 'Great Lakes Observing System'
    nc.institution = 'GLOS.'
    nc.keywords = 'Water Level, Great Lakes'
    nc.references = 'http://www.GLOS.org'
    nc.geospatial_lat_min = '41.53'
    nc.geospatial_lat_max = '48.40'
    nc.geospatial_lon_min = '-92.08'
    nc.geospatial_lon_max = '-76.50'
    nc.publisher_name = 'RPS ASA on behalf of GLOS.'
    nc.publisher_phone = '(401) 789-6224'
    nc.publisher_email = 'devops@asascience.com'
    nc.publisher_url = 'http://www.asascience.com/'
    units = WATER_LEVEL_UNITS

    coordinates = ["time", "lat", "lon"]
    nc.setncattr("featureType", "timeseries")
    var = nc.createVariable('water_level',    "f4", ("time", "station"), fill_value=fillvalue)
    #Set 'coordinates' attribute
    setattr(var, "coordinates", " ".join(coordinates))
    setattr(var, "standard_name", "water_surface_height_above_reference_datum")
    setattr(var, "units", units)
    setattr(var, "description", 'Water Height')
    #Set data
    #rebuilt = np.reshape(data,[len(data[0]),len(data)])
    #rebuilt = np.rot90(data.copy(),-1)
    npdata = np.array(data, dtype=np.float32)

    var[:] = npdata.T

    nc.sync()

    nc.close()

#--------------------------------------------------------------------------------

if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument('--clean', '-c', help= "Cleans the workspace after use.  Will delete all files that start with the word 'page'.")
    #parser.add_argument('--ago', '-a', nargs = '?',  help= "Determines how long ago you want the data from.", default = 1)
    #args = parser.parse_args()
    #when = date.today()-timedelta(int(args.ago))
    #print date.today()
    #print 'Getting Data for ',when
    report_path = data_download()
    ncCreate(1, report_path)
    ncCreate(2, report_path)
    ncCreate(3, report_path)
    ncCreate(4, report_path)
    ncCreate(5, report_path)
    ncCreate(6, report_path)



    #if args.clean:
    #    os.system('rm page*')
    #    os.system('rm report.pdf')


