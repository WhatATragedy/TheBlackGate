import os
import logging
import requests
import datetime
import re
import errno
import pandas as pd

class TalConsumer:
    def __init__(self):
        self._tal_url = 'https://ftp.ripe.net/rpki/'
        self._tal_filenames = [
            'afrinic.tal',
            'apnic-afrinic.tal',
            'apnic-arin.tal',
            'apnic-iana.tal',
            'apnic-lacnic.tal',
            'apnic-ripe.tal',
            'apnic.tal',
            'arin.tal',
            'lacnic.tal',
            'localcert.tal',
            'ripencc.tal'
        ]
        self.output_directory = 'tals'
        self.collection_date = datetime.datetime.now().strftime('%Y/%m/%d')
        return None

    def check_output_directory(self, directory):
        relative_path = os.getcwd()
        if directory is not None:
            if os.path.exists(directory):
                return directory
            else:
                os.mkdir(f'{relative_path}\{directory}') 
                return relative_path + directory
        else:
            if os.path.exists(f'{relative_path}\{self.output_directory}'):
                return f'{relative_path}\{self.output_directory}'
            else:
                os.mkdir(f'{relative_path}\{self.output_directory}') 
                return relative_path + self.output_directory

    def get_tal_files(self, tals=None, output_directory=None, date=None):
        self.output_directory = output_directory if output_directory is not None else self.check_output_directory(output_directory)
        collection_date = date if date is not None else self.collection_date
        try:
            os.mkdir(output_directory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                print('Directory Already Exists...')
        if tals is None:
            tals = self._tal_filenames
        tals =  tals if isinstance(tals, list) else [tals]
        for tal in tals:
            print(f'Getting Tals {tal}')
            url = f'{self._tal_url}{tal}/{collection_date}/roas.csv'
            print(url)
            r = requests.get(url)
            open(f'{self.output_directory}/{tal}', 'wb').write(r.content)
        return True

    def tals_to_list(self, tal_directory=None):
        tal_directory = tal_directory if tal_directory is not None else self.output_directory
        tal_values = []
        for filename in os.listdir(tal_directory):
            tal_df = pd.read_csv(f'{tal_directory}/{filename}')
            try:
                tal_values.extend(tal_df[['ASN','IP Prefix', 'Not Before', 'Not After']].values.tolist())
            except KeyError:
                continue
        tal_values_processed = []
        for tal in tal_values:
            asn = tal[0][2:]
            route = tal[1]
            start_date = tal[2].split(" ")[0]
            end_date = tal[3].split(" ")[0]
            tal_values_processed.append([asn, route, start_date, end_date])
        print(zip(tal_values_processed))
        return tal_values_processed

                



    def get_tal_list(self):
        r = requests.get(self._tal_url)
        names = re.findall('<li><a href=".*">([^<]+)</a></li>', r.text, re.IGNORECASE)
        self._tal_filenames = [tal.replace('/','').strip() for tal in names if 'Parent Directory' not in tal]
        return self._tal_filenames


