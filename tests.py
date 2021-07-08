from ChordNode import ChordNode
from Utils import Utils
from unittest import TestCase, main


class TestChord(TestCase):
    def test_default(self):
        cmds = []
        for i in range(6):
            ch = ChordNode(
                NODE_ID="ch-" + str(i),
                CHORD_ID=i,
                LOG_PATH_HOST="/home/nacho/Documents/test/storage/logs",
                CHORD_KEYS_PER_NODE=3,
                CHORD_TOTAL_OF_NODES=6,
                CHORD_NUMBER_OF_PREDECESSORS=0,
                CHORD_NUMBER_OF_SUCCESSORS=0,
                CHORD_LOOKUP_POLICY='scalable',
                POOL_ID='pool-xxxx',
                RABBITMQ_HOST="10.0.0.4"
            )
            cmds.append(ch.generate_cmd())
        Utils.runCommands(cmds)


if __name__ == '__main__':
    main()
