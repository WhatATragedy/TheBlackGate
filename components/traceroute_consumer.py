import datetime
import requests
import bz2
import os
import shutil
import json
import ijson
import string
import random
from tqdm import tqdm
import datetime
from components.neo import NeoInterface

class TracerouteConsumer():
    def __init__(self, output_dir=None, max_files=2):
        self._url = 'https://data-store.ripe.net/datasets'
        self.datetime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self._intervals = [str(number).rjust(4, '0') for number in range(0,2400, 100)]
        self.output_dir = '/tmp/' if output_dir is None else output_dir
        self.max_files = max_files
        
    def ran_gen(self, size, chars=string.ascii_uppercase + string.digits): 
        return ''.join(random.choice(chars) for x in range(size)) 

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
        #todo this should really return a list of the files created
        return True

    def decompress_files(self):
        for file_name in os.listdir(self.output_dir):
            if 'tracert' in file_name:
                with bz2.BZ2File(f'{self.output_dir}/{file_name}') as fr, open(f'{self.output_dir}/{file_name[:-4]}',"wb") as fw:
                    shutil.copyfileobj(fr,fw,length = 1000000)      
        return True

    def purge_files(self):
        for file_name in os.listdir(self.output_dir):
            if 'tracert' in file_name:
                os.remove(f'{self.output_dir}/{file_name}')
        return True
    
    def ingest_to_neo(self, traceroute_file):
        print(datetime.datetime.now())
        neo = NeoInterface(uri="bolt://localhost:7687", user="neo4j", password="neo4j")
        with open(traceroute_file) as traceroute_input:
            parser = ijson.parse(traceroute_input, multiple_values=True)
            index = 0
            results = []
            #traceroute should be a list of results
            #msm_id, src_ip, dest_ip, hop_number, ip_address
            for prefix, event, value in tqdm(parser):
                index = index + 1
                #if index > 1000:
                #   break
                #print('prefix={}, event={}, value={}'.format(prefix, event, value))
                if prefix is '' and event == 'start_map':
                    ##this is the start of an object so output traceroute object and flush
                    #print('Start of object')
                    random_id = self.ran_gen(6)
                    results = []
                if prefix is '' and event == 'end_map':
                    neo.traceroute_to_transaction(results)
                #if prefix == 'result' and event == 'start_array':
                    ##this is the start of the results array to include hops
                    #print('Start of results Array')
                if prefix == 'result.item.hop':
                    hop = value
                if prefix == 'result.item.result.item.from':
                    hop_ip = value
                if prefix == 'src_addr':
                    src_ip = value
                if prefix == 'dst_addr':
                    dst_ip = value
                if prefix == 'result.item.result' and event == 'end_array':
                    #we have just looped an array so add the hop to results
                    #print('End Of Hop Array')
                    results.append((random_id, src_ip, dst_ip, hop, hop_ip))      
        print(datetime.datetime.now())
        return results

   
if __name__ == "__main__":
    trc = TracerouteConsumer(max_files=1)
    trc.get_traceroutes()
    trc.decompress_files()
    #trc.purge_files()