import glob
import argparse
from dateutil.parser import parse
import os
def main(basepath):
    for file in glob.glob(basepath + '/*.nc'):
        suffix = file.split('_')[-1]
        date_str = suffix[0:8]
        dtg = parse(date_str).strftime('%Y-%m')
        new_filename = '_'.join(file.split('_')[:-1])
        new_filename = '_'.join([new_filename, dtg + '.nc'])
        print "Moving %s to %s" % (file, new_filename)
        os.rename(file, new_filename)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="rename some files")
    parser.add_argument('basepath', help='basepath')
    args = parser.parse_args()

    main(args.basepath)
    
