from Utils import Utils


class StorageNode(object):
    def __init__(self, **kwargs):
        self.network = kwargs.pop('network', 'mynet')
        self.index = kwargs.pop("group_index", 0)
        self.metadata = kwargs.pop("metadata")
        self.label = 'storage'
        #self.docker_image = kwargs.pop('DOCKER_IMAGE','nachocode/storage-node')
        self.data = kwargs

    def build(self, **kwargs):
        self.data = {**self.data, **kwargs}

    def generate_cmd(self, **kwargs):
        del self.data['network']
        self.docker_image = self.data.pop(
            'DOCKER_IMAGE', 'nachocode/storage-node')
        LOG_PATH_HOST = self.data.pop("LOG_PATH_HOST")
        STORAGE_PATH_HOST = self.data.pop("STORAGE_PATH_HOST")
        STORAGE_PATH = self.data["STORAGE_PATH"]
        PORT = self.data["PORT"]
        envs_cmd = Utils.fromDictToDockerCmd(self.data)
        cmd = "docker run --name {0} -d -p {6}:80 --network {1} -l {2} -v {3}:{5} -v {4}:/app/logs".format(
            self.data['NODE_ID'], self.network, self.label, STORAGE_PATH_HOST, LOG_PATH_HOST, STORAGE_PATH, PORT)
        cmd += envs_cmd + " " + self.docker_image
        # print(cmd)
        # print("_"*50)
        return cmd

    def addVolumen(self, **kwargs):
        pass
