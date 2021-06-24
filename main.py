import sys
import os
import subprocess
from hashlib import sha1,md5
from random import randint


def fromDictToDockerCmd(data):
    envs = ""
    for key,value in data.items():
        envs += " -e {}={}".format(key,value)
    return envs

def arrayToEnv(key,length):
    data = []
    for i in range(length):
        data.append('{}.{}'.format(key,i))
    return data

def envWithValues(key,data):
    xs = arrayToEnv(key,len(data))
    result = {}
    for key, data in zip(xs,data):
        for k,v in data.items():
            if not(k == ""):
                newKey = key+"."+k
                result[newKey] = v
            else: 
                result[key] = v 
    return result

def nodesToEnv(key:str,nodes:list,fnAttrs):
    for i, node in enumerate(nodes):
        peers = list(set(nodes).difference(set([node])))
        #peerData = list(map( lambda x: {'':x.data['NODE_ID']},peers))
        peerData = list(map(fnAttrs,peers))
        d = envWithValues(key, peerData )
        node.build(**d)
    return nodes

class BullyNode(object):
    def __init__(self,**kwargs):
        self.network       = kwargs.pop('network','mynet')
        self.log_path_host = kwargs.pop("LOG_PATH_HOST") 
        self.label         = 'bully'
        self.docker_image  = 'nachocode/bully-node'
        self.data          = kwargs
    def build(self,**kwargs):
        self.data={**self.data,**kwargs}
    def generate_cmd(self,**kwargs):
        envs_cmd =  fromDictToDockerCmd(self.data)
        cmd      = "docker run --name {} -d --network {} -l {} -v {}:/app/logs".format(self.data['NODE_ID'],self.network,self.label,self.log_path_host)
        cmd      += envs_cmd +" "+ self.docker_image
        #print(cmd)
        #print("_"*50)
        return cmd
    def __str__(self):
        return 'BullyNode(NODE_ID={},PRIORITY={},IS_LEADER={})'.format(self.data['NODE_ID'],self.data['PRIORITY'],self.data['IS_LEADER'])

class StorageNode(object):
    def __init__(self,**kwargs):
        self.network  = kwargs.pop('network','mynet')
        self.index    = kwargs.pop("group_index",0)
        self.metadata = kwargs.pop("metadata")
        self.label    = 'storage'
        self.docker_image = 'nachocode/storage-node'
        self.data = kwargs
    def build(self,**kwargs):
        self.data = {**self.data,**kwargs}
    def generate_cmd(self,**kwargs):
        del self.data['network']
        LOG_PATH_HOST     = self.data.pop("LOG_PATH_HOST")
        STORAGE_PATH_HOST = self.data.pop("STORAGE_PATH_HOST")
        STORAGE_PATH      = self.data["STORAGE_PATH"]
        PORT              = self.data["PORT"]
        envs_cmd          = fromDictToDockerCmd(self.data)
        cmd      = "docker run --name {0} -d -p {6}:80 --network {1} -l {2} -v {3}:{5} -v {4}:/app/logs".format(self.data['NODE_ID'],self.network,self.label,STORAGE_PATH_HOST,LOG_PATH_HOST,STORAGE_PATH,PORT)
        cmd      += envs_cmd +" "+ self.docker_image
        #print(cmd)
        #print("_"*50)
        return cmd

    def addVolumen(self,**kwargs):
        pass

def buildBully(nodes,metadata,**kwargs):
    bully_nodes        = []
    bully_node_leader  = None
    node_leader        = None
    COORDINATOR_WINDOW = kwargs.get("COORDINATOR_WINDOWS",3000)
    for i,node in enumerate(nodes):
        originalNodeId = node.data["NODE_ID"]
        priority       = node.metadata['priority']
        is_leader      = node.metadata['is_leader']
        rabbitmq_host  = node.data['RABBITMQ_HOST']
        pool_id        = node.data['POOL_ID'] 
        LOG_PATH_HOST  = node.data['LOG_PATH_HOST']
        LOG_PATH_DOCKER = "/app/logs"
        bully_node = BullyNode(NODE_ID = originalNodeId.replace("sn","cs"),PRIORITY = priority,RABBITMQ_HOST=rabbitmq_host,POOL_ID = pool_id,IS_LEADER=is_leader,NODE = originalNodeId,LOG_PATH=LOG_PATH_DOCKER,LOG_PATH_HOST=LOG_PATH_HOST,COORDINATOR_WINDOW=COORDINATOR_WINDOW,**metadata)
        bully_nodes.append(bully_node)
        if(is_leader =='true'):
            bully_node_leader = bully_node
            node_leader       = node
    for i,node in enumerate(bully_nodes):
        node.build(SHADOW_LEADER_NODE = bully_node_leader.data['NODE_ID'],LEADER_NODE = node_leader.data['NODE_ID'])
    bully_nodes = nodesToEnv("BULLY_NODES",bully_nodes,lambda x:{'node-id':x.data['NODE_ID'],'priority':x.data['PRIORITY']  })
    return bully_nodes
    #print(bully_nodes[0].data)


class StorageNodeGroup(object):
    def __init__(self,**kwargs):
        self.nodes         = kwargs.get("storage_nodes",[])
        self.auto_id       = kwargs.pop('auto_id',False)
        self.base_port     = kwargs.pop("BASE_PORT",4000)
        self.shared_config = kwargs.pop("shared_config",{})
        self.consensus     = kwargs.pop("consensus")
        self.dictConsensus = {'BULLY':buildBully}
        self.cmds = []

    def generateId(self,i):
        if(self.auto_id):
            hasher = md5()
            hasher.update(str(i).encode())
            nodeId = "sn-"+hasher.hexdigest()[:10]
        else:
            nodeId = "sn-"+str(i)
        return nodeId

    def build(self,**kwargs):
        for i,node in enumerate(self.nodes):
            node.build(**self.shared_config)
            nodeId              = self.generateId(i)
            STORAGE_PATH        = node.data["STORAGE_PATH"].replace("{NODE_ID}",nodeId)
            STORAGE_PATH_DOCKER = "/usr/share/nginx/html"
            LOG_PATH_HOST       = node.data['LOG_PATH']
            LOG_PATH_DOCKER     = "/app/logs"
            PORT                = i + self.base_port
            newData             = {"NODE_ID":nodeId,"STORAGE_PATH":STORAGE_PATH_DOCKER,"STORAGE_PATH_HOST":STORAGE_PATH,"LOG_PATH":LOG_PATH_DOCKER,"LOG_PATH_HOST":LOG_PATH_HOST,"PORT":PORT}
            node.build(**newData)
        self.nodes = nodesToEnv("STORAGE_NODES",self.nodes,lambda x:{'':x.data['NODE_ID']})
        #print(self.nodes)
        self.consensus_nodes = self.dictConsensus.get(self.consensus['algorithm'],lambda x,y:[])(self.nodes,self.consensus['metadata'])
        self.consensus_nodes = nodesToEnv("NODES",self.consensus_nodes,lambda x:{'':x.data['NODE']})
        #print(self.consensus_nodes)
        for node in self.nodes+self.consensus_nodes:
            self.cmds.append(node.generate_cmd())
    def run(self):
        for cmd in self.cmds:
            p = subprocess.Popen(cmd.split(),stdout = subprocess.PIPE)
            output,error =  p.communicate()
            print(output,error)

if __name__ =='__main__':
    total_storage_nodes = int(sys.argv[1])
    print("TOTAL OF STORAGE_NODES = {}".format(total_storage_nodes))

    storages_nodes = []
    for i in range(total_storage_nodes):
        is_leader = 'true' if(i==total_storage_nodes-1) else 'false'
        storages_nodes.append(StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':i,'is_leader':is_leader} ))


    sns = StorageNodeGroup(
            auto_id= False,
            storage_nodes = storages_nodes,
            shared_config = {
                "LOADER_BALANCER":"RB",
                "network":"mynet",
                "RABBITMQ_HOST":"10.0.0.4",
                "HEARTBEAT_TIME":250,
                "POOL_ID":"pool-xxxx",
                "REPLICATION_FACTOR":2,
                "LOG_PATH":"/home/nacho/Documents/test/storage/logs"
            },
            consensus = {
                'algorithm': "BULLY", # BULLY / CHORD / PAXOS / RAFT 
                'metadata':{
                    'MAX_RETRIES':3,
                    'HEALTH_CHECK_TIME':1000
                    #'MAX_RETRIES_OK_MESSAGES':5,
                }
            }
    )

    sns.build()
    sns.run()
