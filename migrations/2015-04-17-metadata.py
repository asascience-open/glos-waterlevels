#!/usr/bin/env python
'''
migrations/2015-04-17-metadata.py

Migration to update the metadata for existing netCDF files to include the new comments, acknowledgements etc.
'''

from netCDF4 import Dataset
from glob import glob
import os


def apply_metadata(nc):
    nc.setncattr("creator_email", "hhpm@usace.army.mil")
    nc.setncattr("creator_url", "http://www.lre.usace.army.mil/Missions/GreatLakesInformation/GreatLakesWaterLevels.aspx")
    nc.setncattr("acknowledgement", 'Daily mean Great Lakes water levels are harvested from the Great Lakes Water Level reports (link) on the Great Lakes Information pages (link) maintained by the Detroit District of the US Army Corps of Engineers (link). The mean water levels for each of the Great Lakes is calculated from individual water level sensors according to guidance provided by the Coordinating Committee on Great Lakes Basic Hydraulic and Hydrologic Data.')
    nc.setncattr('institution', 'US Army Corps of Engineers')
    nc.setncattr('comment', 'These data are harvested with permission from the U.S. Army Corps of Engineers\' Great Lakes Water Levels Reports. (link to http://www.lre.usace.army.mil/Missions/GreatLakesInformation/GreatLakesWaterLevels/CurrentConditions.aspx)')


def main(args):
    
    for path in glob(os.path.join(args.path, '**/*.nc')):
        with Dataset(path, 'r+') as nc:
            apply_metadata(nc)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Migration script to apply new metadata to the GLOS Water Levels datasets")
    parser.add_argument('path', help='Path to the root dataset directory containing all the netCDF files')
    args = parser.parse_args()
    main(args)
