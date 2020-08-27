from bs4 import BeautifulSoup
import pandas
import requests
import mrtparse
import datetime
import os
import errno
import bz2
from bgpdump import BgpDump
from multiprocessing import Pool
import itertools

class RibConsumer():
    def __init__(self):
        self._peering_status = 'http://www.routeviews.org/peers/peering-status.html'
        self._collectors = self._get_routeview_collectors()
        self._rib_endpoint = 'http://archive.routeviews.org/'
        #http://archive.routeviews.org/route-views.amsix/bgpdata/
        return None
    
    @staticmethod
    def _get_routeview_collectors():
        r = requests.get('http://www.routeviews.org/routeviews/index.php/collectors/')
        rv_collector_df = pandas.read_html(r.text)[0].dropna(axis=0, thresh=4)
        results  = rv_collector_df.loc[rv_collector_df['UI'] == 'telnet', 'Host'].tolist()
        results = [result.replace('.routeviews.org', '') for result in results]
        return results
        
    def _get_routeview_devices(self):
        r = requests.get('http://archive.routeviews.org/')
        soup = BeautifulSoup(r.text, 'html.parser')
        results = soup.find('li', string='Data Archives')
        print (results)

    @staticmethod
    def create_intervals(limit=2400):
        intervals = []
        for interval in range(0,limit, 100):
            interval = str(interval).rjust(4, '0')
            print(interval)
            intervals.append(interval)
        return intervals

    def get_ribs(self,output_directory, date=None, intervals=None, collector_list=None):
        #http://archive.routeviews.org/route-views.amsix/bgpdata/2020.06/RIBS/rib.20200601.0000.bz2
        intervals = intervals if intervals is not None else self.create_intervals()
        date = date if date is not None else datetime.datetime.now() - datetime.timedelta(days = 1)
        collector_list = collector_list if collector_list is not None else self._collectors
        try:
            os.mkdir(output_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                print('Directory Already Exists...')
        ##create list of tuples for star map
        pool_args = []
        for collector, interval in itertools.product(collector_list, intervals):
            print(collector, interval)
            pool_args.append((collector, interval, output_directory, date))
        print(pool_args)
        with Pool(3) as pool:
           pool.starmap(self.map_collections_to_process, pool_args)

    def map_collections_to_process(self, collector, interval, output_directory, date):
        dirDate = date.strftime('%Y.%m')
        fileDate = date.strftime('%Y%m%d')
        try:
            os.mkdir(f'{output_directory}/{collector}')
        except OSError as e:
            if e.errno == errno.EEXIST:
                print('Directory Already Exists...')
        for filename in os.listdir(f'{output_directory}/{collector}'):
            os.remove(f'{output_directory}/{collector}/{filename}')
        print(f'Getting Ribs - {collector}: {interval}')
        url = f'{self._rib_endpoint}{collector}/bgpdata/{dirDate}/RIBS/rib.{fileDate}.{interval}.bz2'
        r = requests.get(url)
        print(f'Completed Collection of {collector}: {interval}')
        try:
            decompress_data = bz2.decompress(r.content)
            open(f'{output_directory}/{collector}/rib.{fileDate}.{interval}.decomp', 'wb').write(decompress_data)
            mrt_data = mrtparse.Reader(f'{output_directory}/{collector}/rib.{fileDate}.{interval}.decomp')
            count = 0
            for m in mrt_data:
                m = m.mrt
                if m.err:
                    continue
                b = BgpDump(open(f'{output_directory}/{collector}/rib.{fileDate}.{interval}', 'a'))
                if m.type == mrtparse.MRT_T['TABLE_DUMP']:
                    b.td(m, count)
                elif m.type == mrtparse.MRT_T['TABLE_DUMP_V2']:
                    #this is the one used
                    b.td_v2(m)
                elif m.type == mrtparse.MRT_T['BGP4MP']:
                    b.bgp4mp(m, count)
                count += 1
            print(f'Completed MRT Parse of {collector}: {interval}')
            os.remove(f'{output_directory}/{collector}/rib.{fileDate}.{interval}.decomp')
        except Exception as e:
            print(f'Error Parsing File Contents for {collector}:{interval}: {e}')

    def ribs_to_list(self, ribs_directory):
        ribs_directory = ribs_directory if ribs_directory is not None else 'ribs'
        for directory in ribs_directory:
            return False


    def cleanup_ribs(self, output_directory):
        os.remove(output_directory)

if __name__ == '__main__':
    print('Oof')



