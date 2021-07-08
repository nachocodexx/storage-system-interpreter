# from StorageNode import StorageNode
import subprocess


class Utils(object):
    @staticmethod
    def fromDictToDockerCmd(data):
        envs = ""
        for key, value in data.items():
            envs += " -e {}={}".format(key, value)
        return envs

    @staticmethod
    def arrayToEnv(key, length):
        data = []
        for i in range(length):
            data.append('{}.{}'.format(key, i))
        return data

    @staticmethod
    def envWithValues(key, data):
        xs = Utils.arrayToEnv(key, len(data))
        result = {}
        for key, data in zip(xs, data):
            for k, v in data.items():
                if not(k == ""):
                    newKey = key + "." + k
                    result[newKey] = v
                else:
                    result[key] = v
        return result

    @staticmethod
    def nodesToEnv(key: str, nodes: list, fnAttrs):
        for i, node in enumerate(nodes):
            peers = list(set(nodes).difference(set([node])))
            #peerData = list(map( lambda x: {'':x.data['NODE_ID']},peers))
            peerData = list(map(fnAttrs, peers))
            d = Utils.envWithValues(key, peerData)
            node.build(**d)
        return nodes

    @staticmethod
    def runCommands(cmds):
        for cmd in cmds:
            p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            output, error = p.communicate()
            print(output, error)

    @staticmethod
    def createBasicStorageSystem(**kwargs):
        total_storage_nodes = kwargs.get("storage_nodes", 3)
        StorageNode = kwargs.get("StorageNodeClass")
        StorageNodeGroup = kwargs.get("StorageNodeGroupClass")
        print("TOTAL OF STORAGE_NODES = {}".format(total_storage_nodes))
        storages_nodes = []
        for i in range(total_storage_nodes):
            is_leader = 'true' if(i == total_storage_nodes - 1) else 'false'
            storages_nodes.append(StorageNode(
                STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}", metadata={'priority': i, 'is_leader': is_leader}))

        sns = StorageNodeGroup(
            auto_id=False,
            storage_nodes=storages_nodes,
            shared_config={
                "LOADER_BALANCER": "RB",
                "network": "mynet",
                "RABBITMQ_HOST": "10.0.0.4",
                "HEARTBEAT_TIME": 250,
                # "HEARTBEAT_TIME":100,
                "POOL_ID": "pool-xxxx",
                "REPLICATION_FACTOR": 2,
                "LOG_PATH": "/home/nacho/Documents/test/storage/logs",
                "DOCKER_IMAGE": "nachocode/storage-node:v3",
                "REPLICATION_STRATEGY": 'active'  # passive / active
            },
            consensus={
                'algorithm': "BULLY",  # BULLY / CHORD / PAXOS / RAFT
                'metadata': {
                    'MAX_RETRIES': 5,
                    'HEALTH_CHECK_TIME': 1000
                    # 'MAX_RETRIES_OK_MESSAGES':5,
                }
            }
        )

        sns.build()
        sns.run()
