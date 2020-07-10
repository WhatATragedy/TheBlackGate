from components.tal import TalConsumer
from components.rib import RibConsumer
from components.traceroute_consumer import TracerouteConsumer
from  components.neo import NeoInterface

#tal = TalConsumer()
#items = tal.get_tal_list()
if __name__ == '__main__':
    rib = RibConsumer()
    rib.get_ribs('ribs', intervals=['0000'], collector_list=['route-views.linx', 'route-views.amsix', 'route-views.kixp', 'route-views.jinx'])
    #tal = TalConsumer()
    #tal_values = tal.tals_to_list()
    #trc = TracerouteConsumer(max_files=1, output_dir='traceroutes')
    #trc.get_traceroutes()
    #trc.decompress_files()
    #results = trc.ingest_to_neo('/home/ec2-user/TheBlackGate/traceroutes/tracert-2020-06-28T0000.')
    

