from unittest import TestCase
from netCDF4 import Dataset
from datetime import datetime

class TestWaterLevels(TestCase):

    def test_water_levels(self):
        urls = ['http://tds.glos.us/thredds/dodsC/water_levels/TheGreatLakes-Agg',
                'http://tds.glos.us/thredds/dodsC/water_levels/LakeSuperior-Agg',
                'http://tds.glos.us/thredds/dodsC/water_levels/LakeOntario-Agg',
                'http://tds.glos.us/thredds/dodsC/water_levels/LakeErie-Agg',
                'http://tds.glos.us/thredds/dodsC/water_levels/LakeStClair-Agg',
                'http://tds.glos.us/thredds/dodsC/water_levels/LakeMichiganAndHuron-Agg'
                ]
        for url in urls:
            print url
            nc = Dataset(url)

            # Make sure we can get the entire data
            data = nc.variables['water_level'][:]

            time_vals = nc.variables['time'][:]

            t0 = time_vals[0]
            tn = time_vals[-1]

            dt0 = datetime.fromtimestamp(t0)
            dtn = datetime.fromtimestamp(tn)
            today = datetime.now()
            
            try:

                assert dt0.month == 4
                assert dt0.year == 2014
                assert dtn.month == today.month
                assert dtn.year == today.year
            except AssertionError:
                print (str(dt0), str(dtn))
                raise

