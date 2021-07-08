from Utils import Utils


class BullyNode(object):
    def __init__(self, **kwargs):
        self.network = kwargs.pop('network', 'mynet')
        self.log_path_host = kwargs.pop("LOG_PATH_HOST")
        self.label = 'bully'
        self.docker_image = 'nachocode/bully-node'
        self.data = kwargs

    def build(self, **kwargs):
        self.data = {**self.data, **kwargs}

    def generate_cmd(self, **kwargs):
        envs_cmd = Utils.fromDictToDockerCmd(self.data)
        cmd = "docker run --name {} -d --network {} -l {} -v {}:/app/logs".format(
            self.data['NODE_ID'], self.network, self.label, self.log_path_host)
        cmd += envs_cmd + " " + self.docker_image
        # print(cmd)
        # print("_"*50)
        return cmd

    @staticmethod
    def createNodes(nodes, metadata, **kwargs):
        bully_nodes = []
        bully_node_leader = None
        node_leader = None
        COORDINATOR_WINDOW = kwargs.get("COORDINATOR_WINDOWS", 3000)
        for i, node in enumerate(nodes):
            originalNodeId = node.data["NODE_ID"]
            priority = node.metadata['priority']
            is_leader = node.metadata['is_leader']
            rabbitmq_host = node.data['RABBITMQ_HOST']
            pool_id = node.data['POOL_ID']
            LOG_PATH_HOST = node.data['LOG_PATH_HOST']
            LOG_PATH_DOCKER = "/app/logs"
            bully_node = BullyNode(NODE_ID=originalNodeId.replace("sn", "cs"), PRIORITY=priority, RABBITMQ_HOST=rabbitmq_host, POOL_ID=pool_id, IS_LEADER=is_leader,
                                   NODE=originalNodeId, LOG_PATH=LOG_PATH_DOCKER, LOG_PATH_HOST=LOG_PATH_HOST, COORDINATOR_WINDOW=COORDINATOR_WINDOW, **metadata)
            bully_nodes.append(bully_node)
            if(is_leader == 'true'):
                bully_node_leader = bully_node
                node_leader = node
        for i, node in enumerate(bully_nodes):
            node.build(
                SHADOW_LEADER_NODE=bully_node_leader.data['NODE_ID'], LEADER_NODE=node_leader.data['NODE_ID'])
        bully_nodes = Utils.nodesToEnv("BULLY_NODES", bully_nodes, lambda x: {
            'node-id': x.data['NODE_ID'], 'priority': x.data['PRIORITY']})
        return bully_nodes

    def __str__(self):
        return 'BullyNode(NODE_ID={},PRIORITY={},IS_LEADER={})'.format(self.data['NODE_ID'], self.data['PRIORITY'], self.data['IS_LEADER'])
