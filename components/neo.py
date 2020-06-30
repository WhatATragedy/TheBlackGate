from py2neo import Graph, Node, Relationship
import time

class NeoInterface:

    def __init__(self, uri, user, password):
        self.graph = Graph(host="localhost", auth=("neo4j", "neo5j")) 

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    def traceroute_to_transaction(self, traceroute):
        print(traceroute)
        for index, traceroute_hop in enumerate(traceroute):
            tx = self.graph.begin()
            if index == 0:
                src_ip = traceroute_hop[1]
                src_node = Node("IPAddress", ip_addr=src_ip)
                tx.create(src_node)

            elif index == len(traceroute):
                dst_ip = traceroute_hop[2]
                dst_node = Node("IPAddress", ip_addr=dst_ip)
                tx.create(dst_node)

            traceroute_id = traceroute_hop[0]
            hop_number = traceroute_hop[3]
            hop_ip = traceroute_hop[4]
            hop_node = Node("IPAddress", ip_addr=hop_ip)
            tx.create(hop_node)
            relation_properties = {
                'hop_number': hop_number,
                'traceroute_id': traceroute_id
            }

            if index == len(traceroute):
                relation = Relationship(hop_node, 'ROUTES_TO', dst_node, **relation_properties)
            elif index == 0:
                relation = Relationship(src_node, 'ROUTES_TO', hop_node, **relation_properties)
            else:
                relation = Relationship(prev_hop, 'ROUTES_TO', hop_node, **relation_properties)
            print(relation)
            tx.create(relation)
            tx.commit()
            prev_hop = hop_node
            print(prev_hop)
            print(hop_node)
            time.sleep(1)
            print(self.graph.exists(relation))
        
if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "password")
    greeter.print_greeting("hello, world")
    greeter.close()
    
