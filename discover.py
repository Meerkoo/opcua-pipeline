import logging
from opcua import Client

client = Client("opc.tcp://172.16.193.222:4842", timeout=50) # Chagne
client.connect()

try:
    root = client.get_root_node()
    print(f"Root node: {root}")

    objects = client.get_objects_node()
    print(f"Objects node: {objects}")

    print("Available nodes:")
    for child in objects.get_children():
        browse_name = child.get_browse_name()
        node_id = child.nodeid
        print(f"- Browse Name: {browse_name.Name}  |  NodeId: {node_id}")
        
        for grandchild in child.get_children():
            grandchild_browse_name = grandchild.get_browse_name()
            grandchild_node_id = grandchild.nodeid
            print(f"    - {grandchild_browse_name.Name}  |  {grandchild_node_id}")

finally:
    client.disconnect()
