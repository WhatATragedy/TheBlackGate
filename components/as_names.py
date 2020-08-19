
import requests
import io 

class ASNNames():
    def __init__(self):
        self.url = 'https://ftp.ripe.net/ripe/asnames/asn.txt'

    def get_asn_names(self):
        asn_name_data = []
        urlData = requests.get('https://ftp.ripe.net/ripe/asnames/asn.txt').content
        rawData = io.StringIO(urlData.decode('utf-8'))
        for line in rawData:
            if len(line.split(',')) >= 3:
                asn_num_and_name, asn_country = line.rsplit(',', 1)
                asn_num, asn_name = asn_num_and_name.split(' ', 1)
            else:
                values = line.split(' ', 1)
                asn_number = values[0]
                if len(values[1].split(',')) == 2:
                    asn_name, asn_country = values[1].split(',')
                else:
                    asn_name = values[1]
                    asn_country = 'None'
            asn_name_data.append({
                'asn': asn_number,
                'name': asn_name,
                'country': asn_country.replace('\n', '').replace(' ', '')
            })  
        return asn_name_data
