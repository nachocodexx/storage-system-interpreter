import sys
import os
import subprocess
from hashlib import sha1,md5



class Nodes(object):
    def __init__(self):
        self.cmds = []
        self.nodes = {}

    def convertToArray(self,key,values):
        data = {}
        for i,value in enumerate(values):
            for attr,value in value.items():
                _key = '{}.{}.{}'.format(key,i,attr)
                data[_key] = value
        return data
    def fromDictToDockerCmd(self,data):
        envs = ""
        for key,value in data.items():
            envs += " -e {}={}".format(key,value)
        return envs

    def storage(self,**kwargs):
        node_id = kwargs.get('node_id')
        self.nodes[node_id] = kwargs
        network = kwargs.get('network','mynet')
        envs = {
                'RABBITMQ_HOST':kwargs.get("rabbitmq_host"),
                'NODE_ID':node_id,
                'LOADER_BALANCER':kwargs.get('load_balancer'),
                'REPLICATION_FACTOR':kwargs.get('replication_factor'),
                'POOL_ID':kwargs.get('pool_id'),
                'STORAGE_PATH':kwargs.get('storage_path'),
                #'STORAGE_NODES':kwargs.get('storage_nodes',[]),
                'HEARTBEAT_TIME':kwargs.get('heartbeat_time',1)
        }
        storage_nodes = self.convertToArray("STORAGE_NODES",kwargs.get("storage_nodes",[]))
        envs = {**envs, **storage_nodes}
        envs_cmd = self.fromDictToDockerCmd(envs)
        cmd = "docker run --name {} -d --network {} -l {}".format(node_id,network,'storage')
        cmd += envs_cmd + " nachocode/storage-node"
        self.cmds.append(cmd)

        #print(storage_nodes)

    def bully(self,**kwargs):
        node_id = kwargs.get('node_id')
        self.nodes[node_id] = kwargs
        network = kwargs.get('network','mynet')
        envs = {
                'RABBITMQ_HOST':kwargs.get("rabbitmq_host"),
                'NODE_ID':node_id,
                'NODE':kwargs.get('node'),
                'PRIORITY':kwargs.get('priority'),
                'POOL_ID':kwargs.get('pool_id'),
                "IS_LEADER":kwargs.get('is_leader'),
                'LEADER_NODE':kwargs.get('leader_node'),
                'SHADOW_LEADER_NODE':kwargs.get('shadow_leader'),
                'MAX_RETRIES':10,
                'MAX_RETRIES_OK_MESSAGES':10,
                'HEALTH_CHECK_TIME':3000
        }
        bully_nodes =self.convertToArray("BULLY_NODES",kwargs.get("bully_nodes"))
        print(bully_nodes)
        envs = {**envs, **bully_nodes}
        envs_cmd = self.fromDictToDockerCmd(envs)
        
        cmd = "docker run --name {} -d --network {} -l {}".format(node_id,network,'bully')
        cmd += envs_cmd + " nachocode/bully-node"
        self.cmds.append(cmd)
    def run(self):
        for cmd in self.cmds:
            p = subprocess.Popen(cmd.split(),stdout = subprocess.PIPE)
            output,error =  p.communicate()
            print(output,error)

        #print(output)
        #print(error)

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
        print(cmd)
        print("_"*50)
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
        envs_cmd          = fromDictToDockerCmd(self.data)
        cmd      = "docker run --name {} -d --network {} -l {} -v {}:/app/data -v {}:/app/logs".format(self.data['NODE_ID'],self.network,self.label,STORAGE_PATH_HOST,LOG_PATH_HOST)
        cmd      += envs_cmd +" "+ self.docker_image
        print(cmd)
        print("_"*50)
        return cmd

    def addVolumen(self,**kwargs):
        pass

def buildBully(nodes,metadata):
    bully_nodes       = []
    bully_node_leader = None
    node_leader       = None
    for i,node in enumerate(nodes):
        originalNodeId = node.data["NODE_ID"]
        priority       = node.metadata['priority']
        is_leader      = node.metadata['is_leader']
        rabbitmq_host  = node.data['RABBITMQ_HOST']
        pool_id        = node.data['POOL_ID'] 
        LOG_PATH_HOST  = node.data['LOG_PATH_HOST']
        LOG_PATH_DOCKER = "/app/logs"
        bully_node = BullyNode(NODE_ID = originalNodeId.replace("sn","cs"),PRIORITY = priority,RABBITMQ_HOST=rabbitmq_host,POOL_ID = pool_id,IS_LEADER=is_leader,NODE = originalNodeId,LOG_PATH=LOG_PATH_DOCKER,LOG_PATH_HOST=LOG_PATH_HOST,**metadata)
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
        self.nodes = kwargs.get("storage_nodes",[])
        self.auto_id = kwargs.pop('auto_id',False)
        self.shared_config =kwargs.pop("shared_config",{})
        self.consensus = kwargs.pop("consensus")
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
            STORAGE_PATH_DOCKER = "/app/data"
            LOG_PATH_HOST       = node.data['LOG_PATH']
            LOG_PATH_DOCKER     = "/app/logs"
            newData             = {"NODE_ID":nodeId,"STORAGE_PATH":STORAGE_PATH_DOCKER,"STORAGE_PATH_HOST":STORAGE_PATH,"LOG_PATH":LOG_PATH_DOCKER,"LOG_PATH_HOST":LOG_PATH_HOST}
            node.build(**newData)
        self.nodes = nodesToEnv("STORAGE_NODES",self.nodes,lambda x:{'':x.data['NODE_ID']})
        self.consensus_nodes= self.dictConsensus[self.consensus['algorithm']](self.nodes,self.consensus['metadata'])
        for node in self.nodes+self.consensus_nodes:
            self.cmds.append(node.generate_cmd())
    def run(self):
        for cmd in self.cmds:
            p = subprocess.Popen(cmd.split(),stdout = subprocess.PIPE)
            output,error =  p.communicate()
            print(output,error)

if __name__ =='__main__':
    sn00 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':0,'is_leader':'false'} )
    sn01 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':1,'is_leader':'false'} )
    sn02 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':2,'is_leader':'false'} )
    sn03 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':3,'is_leader':'true'} )
    sn04 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':4} )
    sn05 = StorageNode(STORAGE_PATH="/home/nacho/Documents/test/storage/{NODE_ID}",metadata = {'priority':5} )

    sns = StorageNodeGroup(
            auto_id= False,
            storage_nodes = [sn00,sn01,sn02,sn03],
            shared_config = {
                "LOADER_BALANCER":"RB",
                "network":"mynet",
                "RABBITMQ_HOST":"10.0.0.4",
                "HEARTBEAT_TIME":500,
                "POOL_ID":"pool-xxxx",
                "REPLICATION_FACTOR":2,
                "LOG_PATH":"/home/nacho/Documents/test/storage/logs"
            },
            consensus = {
                'algorithm': "BULLY",
                'metadata':{
                    'MAX_RETRIES':3,
                    'MAX_RETRIES_OK_MESSAGES':10,
                    'HEALTH_CHECK_TIME':1000
                }
            }
    )

    sns.build()
    sns.run()
    #N = Nodes()


"""
    N.bully(
            rabbitmq_host="10.0.0.4",
            node_id="cs-wwww",
            node="sn-wwww",
            priority=-1,
            pool_id='pool-xxxx',
            is_leader = 'false',
            leader_node="sn-xxxx",
            shadow_leader="cs-xxxx",
            bully_nodes =[
                {'node-id':'cs-yyyy','priority':1},
                {'node-id':'cs-zzzz','priority':2},
                #{'node-id':'cs-zzzz','priority':0}
            ]
    )
    N.bully(
            rabbitmq_host="10.0.0.4",
            node_id="cs-xxxx",
            node="sn-xxxx",
            priority=0,
            pool_id='pool-xxxx',
            is_leader = 'true',
            leader_node="sn-yyyy",
            shadow_leader="cs-yyyy",
            bully_nodes =[
                {'node-id':'cs-yyyy','priority':1},
                {'node-id':'cs-zzzz','priority':2},
                #{'node-id':'cs-zzzz','priority':0}
            ]
    )

    N.bully(
            rabbitmq_host="10.0.0.4",
            node_id="cs-yyyy",
            node="sn-yyyy",
            priority=1,
            pool_id='pool-xxxx',
            is_leader = 'false',

            leader_node="sn-xxxx",
            shadow_leader="cs-xxxx",
            bully_nodes =[
                {'node-id':'cs-xxxx','priority':0},
                {'node-id':'cs-zzzz','priority':2},
                #{'node-id':'cs-zzzz','priority':0}
            ]
    )
    N.bully(
            rabbitmq_host="10.0.0.4",
            node_id="cs-zzzz",
            node="sn-zzzz",
            priority=2,
            pool_id='pool-xxxx',
            is_leader = 'false',
            leader_node="sn-xxxx",
            shadow_leader="cs-xxxx",
            bully_nodes =[
                {'node-id':'cs-xxxx','priority':0},
                {'node-id':'cs-yyyy','priority':1},
                #{'node-id':'cs-zzzz','priority':0}
            ]
    )
    N.storage(
            rabbitmq_host="10.0.0.4",
            node_id="sn-yyyy",
            load_balancer = 'RB',
            replication_factor = 2,
            pool_id = 'pool-xxxx',
            storage_path = '/home/nacho/Documents/test/storage/sn-yyyy'
    )
    N.storage(
            rabbitmq_host="10.0.0.4",
            node_id="sn-zzzz",
            load_balancer = 'RB',
            replication_factor = 2,
            pool_id = 'pool-xxxx',
            storage_path = '/home/nacho/Documents/test/storage/sn-zzzz'
    )
    N.storage(
            rabbitmq_host="10.0.0.4",
            node_id="sn-xxxx",
            load_balancer = 'RB',
            replication_factor = 2,
            pool_id = 'pool-xxxx',
            storage_path = '/home/nacho/Documents/test/storage/sn-xxxx'
    )
"""
   # N.run()
    #print(N.nodes)
    #print(N.cmds)
