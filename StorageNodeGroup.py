import os
import subprocess
from BullyNode import BullyNode
from hashlib import sha1, md5
from Utils import Utils


class StorageNodeGroup(object):
    def __init__(self, **kwargs):
        self.nodes = kwargs.get("storage_nodes", [])
        self.auto_id = kwargs.pop('auto_id', False)
        self.base_port = kwargs.pop("BASE_PORT", 4000)
        self.shared_config = kwargs.pop("shared_config", {})
        self.consensus = kwargs.pop("consensus")
        self.dictConsensus = {'BULLY': BullyNode.createNodes}
        self.cmds = []

    def generateId(self, i):
        if(self.auto_id):
            hasher = md5()
            hasher.update(str(i).encode())
            nodeId = "sn-" + hasher.hexdigest()[:10]
        else:
            nodeId = "sn-" + str(i)
        return nodeId

    def build(self, **kwargs):
        for i, node in enumerate(self.nodes):
            node.build(**self.shared_config)
            nodeId = self.generateId(i)
            STORAGE_PATH = node.data["STORAGE_PATH"].replace(
                "{NODE_ID}", nodeId)
            STORAGE_PATH_DOCKER = "/usr/share/nginx/html"
            LOG_PATH_HOST = node.data['LOG_PATH']
            LOG_PATH_DOCKER = "/app/logs"
            PORT = i + self.base_port
            newData = {"NODE_ID": nodeId, "STORAGE_PATH": STORAGE_PATH_DOCKER, "STORAGE_PATH_HOST": STORAGE_PATH,
                       "LOG_PATH": LOG_PATH_DOCKER, "LOG_PATH_HOST": LOG_PATH_HOST, "PORT": PORT}
            node.build(**newData)
        self.nodes = Utils.nodesToEnv(
            "STORAGE_NODES", self.nodes, lambda x: {'': x.data['NODE_ID']})
        # print(self.nodes)
        self.consensus_nodes = self.dictConsensus.get(
            self.consensus['algorithm'], lambda x, y: [])(self.nodes, self.consensus['metadata'])
        self.consensus_nodes = Utils.nodesToEnv(
            "NODES", self.consensus_nodes, lambda x: {'': x.data['NODE']})
        # print(self.consensus_nodes)
        for node in self.nodes + self.consensus_nodes:
            self.cmds.append(node.generate_cmd())

    def run(self):
        for cmd in self.cmds:
            p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            output, error = p.communicate()
            print(output, error)
