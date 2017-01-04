# encoding=utf-8

import json
import re


class ComplexEncoder(json.JSONEncoder):
    def default(self, o):
        if hasattr(o, 'reprJSON'):
            return o.reprJSON()
        if isinstance(o, set):
            ret = []
            for item in o:
                ret.append(json.JSONEncoder.encode(item))
            return ret
        return json.JSONEncoder.default(self, o)


class Node:
    def __init__(self, name):
        self.name = name

    def __hash__(self):
        return hash(self.getName())

    def __repr__(self):
        return self.getName()

    def reprJSON(self):
        return self.getName()

    def getName(self):
        return self.name


def testJsonDumps():
    data = dict()
    aNode = Node('a')
    bNode = Node('b')
    cNode = Node('c')
    data['a'] = []
    data[aNode].append(bNode)
    data[aNode].append(cNode)
    print json.dumps(data, cls=ComplexEncoder)


def testLambda():
    a = dict()
    a['a'] = set()
    a['a'].add(1)
    a['a'].add(2)
    a['a'].add(3)
    a['a'].add(4)
    a['b'] = set()
    a['b'].add(5)
    a['b'].add(6)
    a['b'].add(7)

    c = set(val for valSet in a.values() for val in valSet)
    print c
    print type(c)
    c = set()
    c.add(1)
    c.add(2)
    c.add(311)
    print c


def testJoin():
    a = ['lkajsd']
    print '*'.join(a)


def testRe():
    line = '2017-01-03 11:01:25,041 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode: Assigned container container_1483412440498_0001_01_000001 of capacity <memory:2048, vCores:1> on host tfs04:38559, which has 1 containers, <memory:2048, vCores:1> used and <memory:6144, vCores:7> available after allocation'
    m = re.search('container [a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+', line, re.I)
    if m:
        print 'match: ', m.group()
    else:
        print 'not match'


def testSplit():
    line = "attemp atempasdf_2324_535_34535"
    array = re.split('=| ', line)
    print array


if __name__ == '__main__':
    testSplit()
