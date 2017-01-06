# encoding=utf-8

from CommonUtils import JOIN_CHAR, getEvents
from S3Graph import Node, constructS3Graph
import json
import sys


class Si3GraphNode:
    def __init__(self, signature, log):
        timestamp, event = log['timestamp'], log['event']
        self.signature = set()
        self.signature.update(signature)
        self.identifiers = {}
        for typeName in self.signature:
            if typeName in event:
                self.identifiers[typeName] = event[typeName]
        self.events = set()
        self.events.add(str(event))
        self.times = set()
        self.times.add(timestamp)

        self.children = set()
        self.links = set()

    def __repr__(self):
        return self.getName()

    def reprJSON(self):
        return JOIN_CHAR.join(self.identifiers.values())

    def getName(self):
        return JOIN_CHAR.join(self.identifiers.values())
        '''
        return '{signature: ' + JOIN_CHAR.join(self.signature) + ', identifiers: ' + str(self.identifiers)\
               + ', events: ' + str(len(self.events)) + ', links: ' + str(len(self.links)) \
               + ', children: ' + str(len(self.children)) + '}'
        '''

    def __hash__(self):
        return hash(self.getName())

    def __eq__(self, other):
        return self.signature == other.signature and self.identifiers == other.identifiers


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


def s3NodeMatchEvent(s3Node, event):
    return s3Node.signature.issubset(event.keys())


def si3NodeMatchEvent(si3Node, event):
    return set(si3Node.identifiers.items()).issubset(event.items())


def updateSi3Graph(Si3Graph, S3Graph, log):
    timestamp, event = log['timestamp'], log['event']
    if not event.keys():
        return
    matchNodes = []
    for s3Node in Si3Graph.keys():
        si3NodeSet = Si3Graph[s3Node]
        for si3Node in si3NodeSet:
            if si3NodeMatchEvent(si3Node, event):
                si3Node.events.add(str(event))
                si3Node.times.add(timestamp)
                matchNodes.append(si3Node)
    if not matchNodes:
        for s3Node in S3Graph.keys():
            if s3NodeMatchEvent(s3Node, event):
                si3Node = Si3GraphNode(s3Node.signature, log)
                if s3Node not in Si3Graph:
                    Si3Graph[s3Node] = set()
                Si3Graph[s3Node].add(si3Node)

    # deal witch child nodes for this event
    bfsSi3Nodes = []
    bfsSi3Nodes.extend(matchNodes)
    while len(bfsSi3Nodes) > 0:
        bfsSi3Node = bfsSi3Nodes.pop()
        for childrenNode in S3Graph[Node(bfsSi3Node.signature)]:
            if not s3NodeMatchEvent(childrenNode, event):
                continue
            if childrenNode not in Si3Graph:
                Si3Graph[childrenNode] = set()

            matchNode = None
            for childrenSi3Node in Si3Graph[childrenNode]:
                if si3NodeMatchEvent(childrenSi3Node, event):
                    matchNode = childrenSi3Node
                    break
            if not matchNode:
                matchNode = Si3GraphNode(childrenNode.signature, log)
                Si3Graph[childrenNode].add(matchNode)

            bfsSi3Nodes.append(matchNode)
            matchNode.events.add(str(event))
            matchNode.times.add(timestamp)
            bfsSi3Node.children.add(matchNode)

    for iNode in matchNodes:
        for jNode in matchNodes:
            if iNode != jNode:
                iNode.links.add(jNode)
                jNode.links.add(iNode)


def getRootNodesFromS3Graph(s3Graph):
    allNodes = set(s3Graph.keys())
    childrenNodes = set()
    for nodes in s3Graph.values():
        childrenNodes.update(nodes)
    return allNodes - childrenNodes


def constructSi3Graph(s3Graph, logs):
    si3Graph = {}
    for log in logs:
        updateSi3Graph(si3Graph, s3Graph, log)

    return si3Graph


def getJsTreeData():
    subLogs, s3Graph = constructS3Graph(getEvents())
    si3Graph = constructSi3Graph(s3Graph, subLogs)
    rootNodes = getRootNodesFromS3Graph(s3Graph)
    rootDatas = constructJSTreeData(si3Graph, rootNodes)

    return rootDatas


def constructJSTreeDataRecursively(data, si3Node):
    data['text'] = si3Node.getName()
    data['times'] = list()
    maxTimestamp, minTimestamp = 0, sys.maxint
    for timestamp in si3Node.times:
        maxTimestamp, minTimestamp = max(maxTimestamp, timestamp), min(minTimestamp, timestamp)
        data['times'].append(timestamp)
    data['children'] = list()
    children = list(si3Node.children)
    for i in range(len(children)):
        data['children'].append(dict())
        tmpMax, tmpMin = constructJSTreeDataRecursively(data['children'][i], children[i])
        maxTimestamp, minTimestamp = max(tmpMax, maxTimestamp), min(tmpMin, minTimestamp)
    return maxTimestamp, minTimestamp


def constructJSTreeData(si3Graph, rootNodes):
    rootDatas = []
    rootSi3Nodes = []
    for rootNode in rootNodes:
        rootSi3Nodes.extend(si3Graph[rootNode])
    for rootSi3Node in rootSi3Nodes:
        rootData = dict()
        maxTimestamp, minTimestamp = constructJSTreeDataRecursively(rootData, rootSi3Node)
        rootData['maxTimestamp'] = maxTimestamp
        rootData['minTimestamp'] = minTimestamp
        rootDatas.append(rootData)
    # jsTreeData['core'] = dict()
    # jsTreeData['core']['data'] = rootsData
    # print 'jsTreeData: ', jsTreeData

    return rootDatas


if __name__ == '__main__':
    print getJsTreeData()
