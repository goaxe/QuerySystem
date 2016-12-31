# encoding=utf-8

from CommonUtils import JOIN_CHAR
from S3Graph import Node
from S3Graph import constructS3Graph, getEvents


class Si3GraphNode:
    def __init__(self, signature, event, isEqual=False):
        self.signature = set()
        self.signature.update(signature)
        self.identifiers = {}
        for typeName in self.signature:
            self.identifiers[typeName] = event[typeName]
        self.events = set()
        self.events.add(str(event))
        self.children = set()
        self.links = set()
        self.isEqual = isEqual

    def __repr__(self):
        return self.getName()

    def getName(self):
        return '{signature: ' + JOIN_CHAR.join(self.signature) + ', identifiers: ' + str(self.identifiers)\
               + ', events: ' + str(len(self.events)) + ', links: ' + str(len(self.links)) \
               + ', children: ' + str(len(self.children)) + '}'

    def __hash__(self):
        return hash(self.getName())

    def __eq__(self, other):
        return self.signature == other.signature and self.identifiers == other.identifiers


def s3NodeMatchEvent(s3Node, event):
    if s3Node.isEqual:
        for typeName in s3Node.signature:
            if typeName in event:
                return True
    return s3Node.signature.issubset(event.keys())


def si3NodeMatchEvent(si3Node, event):
    if si3Node.isEqual:
        for k, v in si3Node.identifiers.items():
            if k in event and event[k] == v:
                return True
    return set(si3Node.identifiers.items()).issubset(event.items())


def updateSi3Graph(Si3Graph, S3Graph, event):
    matchNodes = []
    for s3Node in Si3Graph.keys():
        si3NodeSet = Si3Graph[s3Node]
        for si3Node in si3NodeSet:
            if si3NodeMatchEvent(si3Node, event):
                si3Node.events.add(str(event))
                matchNodes.append(si3Node)
    if not matchNodes:
        for s3Node in S3Graph.keys():
            if s3NodeMatchEvent(s3Node, event):
                si3Node = Si3GraphNode(s3Node.signature, event, s3Node.isEqual)
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
                matchNode = Si3GraphNode(childrenNode.signature, event, childrenNode.isEqual)
                Si3Graph[childrenNode].add(matchNode)

            bfsSi3Nodes.append(matchNode)
            matchNode.events.add(str(event))
            bfsSi3Node.children.add(matchNode)

    for iNode in matchNodes:
        for jNode in matchNodes:
            if iNode != jNode:
                iNode.links.add(jNode)
                jNode.links.add(iNode)


def constructSi3Graph():
    events = getEvents()
    si3Graph = {}
    s3Graph = constructS3Graph()
    print 's3Graph: ', s3Graph

    for event in events:
        updateSi3Graph(si3Graph, s3Graph, event)
        print event
        print si3Graph


if __name__ == '__main__':
    constructSi3Graph()
