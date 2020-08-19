from components.tal import TalConsumer
from components.rib import RibConsumer
from components.traceroute_consumer import TracerouteConsumer
from  components.neo import NeoInterface
from components.mongodb import MongoInterface
from components.as_names import ASNNames
from components.postgres import PostgresInterface

#tal = TalConsumer()
#items = tal.get_tal_list()
if __name__ == '__main__':
    #rib = RibConsumer()
    #print(rib.get_ribs('ribs', intervals=['0000', '0100'], collector_list=['route-views.saopaulo']))
    #rib.get_ribs('ribs', intervals=['0000'], collector_list=['route-views.linx', 'route-views.amsix', 'route-views.kixp', 'route-views.jinx'])
    #rib.get_ribs('ribs', intervals=['0000'], collector_list=['route-views.linx', 'route-views.amsix', 'route-views.jinx'])
    tal = TalConsumer()
    #tal.get_tal_files(output_directory='tals')
    tal_values = tal.tals_to_list()
    #trc = TracerouteConsumer(max_files=1, output_dir='traceroutes')
    #trc.get_traceroutes()
    #trc.decompress_files()
    #results = trc.ingest_to_neo('/home/ec2-user/TheBlackGate/traceroutes/tracert-2020-06-28T0000.')
    #as_names_data = ASNNames().get_asn_names()
    #print(as_names_data)
    #mongo = MongoInterface(host='localhost')
    #mongo.insert_dict(as_names_data, 'routing_info', 'asn_whois')
    postgres = PostgresInterface()
    postgres.check_version()
    postgres.create_tal_table()
    #postgres.create_as_name_table()
    postgres.insert_tals(tal_values)
    #postgres.insert_asn_names(as_names_data)



    

