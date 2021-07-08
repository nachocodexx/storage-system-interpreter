import sys
from random import randint
from Utils import Utils
from StorageNode import StorageNode
from ChordNode import ChordNode
from BullyNode import BullyNode
from StorageNodeGroup import StorageNodeGroup


if __name__ == '__main__':
    total_storage_nodes = int(sys.argv[1])
    Utils.createBasicStorageSystem(
        total_storage_nodes=total_storage_nodes,
        StorageNodeClass=StorageNode,
        StorageNodeGroupClass=StorageNodeGroup
    )
