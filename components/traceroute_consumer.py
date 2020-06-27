import datetime
import requests
import bz2
import os
import shutil

class TracerouteConsumer():
    def __init__(self, output_dir=None, max_files=5):
        self._url = 'https://data-store.ripe.net/datasets'
        self.datetime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self._intervals = [str(number).rjust(4, '0') for number in range(0,2400, 100)]
        self.output_dir = '/tmp/' if output_dir is None else output_dir
        self.max_files = max_files
        
    #this should be a static method
    def get_traceroutes(self, days=1, latest=True, startDate=None):
        if latest is not True:
            print('Some Logic here to get older results')
        else:
            for index, interval in enumerate(self._intervals):
                if index < self.max_files:
                    _url = f'{self._url}/atlas-daily-dumps/{self.datetime}/traceroute-{self.datetime}T{interval}.bz2'
                    print(_url)
                    r = requests.get(_url)
                    with open(f'{self.output_dir}/tracert-{self.datetime}T{interval}.bz2', 'wb') as output_file:
                        output_file.write(r.content)
        return True

    def decompress_files(self):
        for file_name in os.listdir(self.output_dir):
            if 'tracert' in file_name:
                with bz2.BZ2File(f'{self.output_dir}/{file_name}') as fr, open(f'{self.output_dir}/{file_name[:-3]}',"wb") as fw:
                    shutil.copyfileobj(fr,fw,length = 1000000)      
        return True

    def purge_files(self):
        for file_name in os.listdir(self.output_dir):
            if 'tracert' in file_name:
                os.remove(f'{self.output_dir}/{file_name}')
        return True
   
if __name__ == "__main__":
    trc = TracerouteConsumer(max_files=1)
    trc.get_traceroutes()
    trc.decompress_files()
    #trc.purge_files()