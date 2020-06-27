from components.tal import TalConsumer
from components.rib import RibConsumer
from components.postgres import PostgresInterface


#tal = TalConsumer()
#items = tal.get_tal_list()
if __name__ == '__main__':
    #rib = RibConsumer()
    #print(rib.get_ribs('ribs', intervals=['0000', '0100'], collector_list=['route-views.saopaulo']))
    tal = TalConsumer()
    tal_values = tal.tals_to_list()

