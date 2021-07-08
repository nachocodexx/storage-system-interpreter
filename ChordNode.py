from Utils import Utils


class ChordNode(object):
    def __init__(self, **kwargs):
        self.network = kwargs.pop('network', 'mynet')
        self.log_path_host = kwargs.pop("LOG_PATH_HOST")
        self.label = 'chord'
        self.docker_image = 'nachocode/chord-node'
        self.data = kwargs

    def build(self, **kwargs):
        self.data = {**self.data, **kwargs}

    def generate_cmd(self, **kwargs):
        LOG_PATH_DOCKER = "/app/logs"
        self.build(LOG_PATH = LOG_PATH_DOCKER)
        envs_cmd = Utils.fromDictToDockerCmd(self.data)
        cmd = "docker run --name {} -d --network {} -l {} -v {}:{}".format(
                self.data['NODE_ID'], 
                self.network, 
                self.label, 
                self.log_path_host,
                LOG_PATH_DOCKER
        )
        cmd += envs_cmd + " " + self.docker_image
        # print(cmd)
        # print("_"*50)
        return cmd

    def __str__(self):
        return 'ChordNode(NODE_ID={},CHORD_ID={})'.format(self.data['NODE_ID'], self.data['CHORD_ID'])
