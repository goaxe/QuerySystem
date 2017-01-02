# encoding=utf-8

import CommonUtils
from itertools import combinations
import TypeRelationGraph


class Node:
    def __init__(self, name, isEqual=False):
        self.signature = set()
        if isinstance(name, list) or isinstance(name, set):
            self.signature.update(name)
        else:
            self.signature.update(name.split(CommonUtils.JOIN_CHAR))
        self.isEqual = isEqual

    def __repr__(self):
        return self.getName()

    def getName(self):
        return CommonUtils.JOIN_CHAR.join(self.signature)

    def __hash__(self):
        return hash(self.getName())

    def __eq__(self, other):
        return self.signature == other.signature


s3Graph = dict()


def initS3GraphFromTypeRelationGraph(typeRelationGraph):
    for k, v in typeRelationGraph.items():
        keyNode = Node(k)
        s3Graph[keyNode] = set()
        for typeName in typeRelationGraph[k].keys():
            relation = typeRelationGraph[k][typeName]
            # not quite sure relation == 3 is needed
            if relation == 1 or relation == 2:
                s3Graph[keyNode].add(Node(typeName))
    return s3Graph


def get11Subsets(typeRelationGraph):
    typeList = typeRelationGraph.keys()
    subsets = []

    for iType in typeList:
        hasExist = False
        iSet = []
        for subset in subsets:
            if iType in subset:
                hasExist = True
                iSet = subset
        if not hasExist:
            iSet = set()
            iSet.add(iType)
            subsets.append(iSet)
        queue = [iType]
        while len(queue):
            first = queue[0]
            queue.pop()
            for jType in typeList:
                if jType != first and jType in typeRelationGraph[first] \
                        and typeRelationGraph[first][jType] == 1 and jType not in iSet:
                    iSet.add(jType)
                    queue.append(jType)

    while True:
        hasMerged = False
        for iSet in subsets:
            for jSet in subsets:
                if iSet == jSet:
                    continue
                if iSet & jSet:
                    subsets.append(iSet | jSet)
                    subsets.remove(iSet)
                    subsets.remove(jSet)
                    hasMerged = True
                    break
        if not hasMerged:
            break

    return subsets


def mergeNodesInS3Graph(originTypes, newNode):
    for typeName in originTypes:
        s3Graph[newNode].update(s3Graph[Node(typeName)])

    originNodes = []
    for originType in originTypes:
        originNodes.append(Node(originType))
    for originNode in originNodes:
        if originNode in s3Graph.keys():
            del s3Graph[originNode]
    for valueSet in s3Graph.values():
        for originNode in originNodes:
            if originNode in valueSet:
                valueSet.remove(originNode)
                valueSet.add(newNode)

    for node in s3Graph.keys():
        if node in s3Graph[node]:
            s3Graph[node].remove(node)


def merge11Nodes(typeRelationGraph, events):
    subsets = get11Subsets(typeRelationGraph)
    for typeNames in subsets:
        if len(typeNames) == 1:
            continue

        node = Node(typeNames, True)
        s3Graph[node] = set()
        kvList = []

        for event in events:
            signature = set(event.keys()) & set(typeNames)
            if not signature:
                continue

            eventKvs = {}
            for key in signature:
                eventKvs[key] = event[key]
            hasExist = False
            for item in kvList:
                for typeName in signature:
                    if typeName in item and item[typeName] == eventKvs[typeName]:
                        item.update(eventKvs)
                        hasExist = True
                        break
                if hasExist:
                    break
            if not hasExist:
                kvList.append(eventKvs)

        for event in events:
            hasExist = False
            k, v = '', ''
            for key in event.keys():
                if key in typeNames:
                    k, v = key, event[key]
                    del event[key]
                    hasExist = True
            if hasExist:
                for kvDict in kvList:
                    if k in kvDict and kvDict[k] == v:
                        event[node.getName()] = CommonUtils.JOIN_CHAR.join(kvDict[key] for key in (set(node.signature) & set(kvDict.keys())))
                        break

        mergeNodesInS3Graph(typeNames, node)


def getmnSubsets(typeRelationGraph):
    typeList = typeRelationGraph.keys()
    subsets = []

    for iType in typeList:
        hasExist = False
        iSet = []
        for subset in subsets:
            if iType in subset:
                hasExist = True
                iSet = subset
        if not hasExist:
            iSet = set()
            iSet.add(iType)
            subsets.append(iSet)
        queue = [iType]
        while len(queue):
            first = queue[0]
            queue.pop()
            for jType in typeList:
                if jType != first and jType in typeRelationGraph[first] \
                        and typeRelationGraph[first][jType] == 3 and jType not in iSet:
                    ismn = True
                    for item in iSet:
                        if (jType not in typeRelationGraph[item]) \
                                or typeRelationGraph[item][jType] != 3:
                            ismn = False
                            break
                    if ismn:
                        iSet.add(jType)
                        queue.append(jType)
    return subsets


def deleteNodeInS3Graph(deleteNode):
    if deleteNode in s3Graph:
        del s3Graph[deleteNode]
    for valueSet in s3Graph.values():
        if deleteNode in valueSet:
            valueSet.remove(deleteNode)


def deleteTypeInS3Graph(deleteType):
    deleteNodeInS3Graph(Node(deleteType))


def mergeMNNodes(typeRelationGraph, events):
    subsets = getmnSubsets(typeRelationGraph)

    for C in subsets:
        if len(C) == 1:
            continue

        markedNodes = set()
        for event in events:
            signature = C & set(event.keys())
            if not signature:
                continue
            exist = False
            for node in s3Graph.keys():
                if node.signature == signature:
                    exist = True
            if exist:
                markedNodes.add(Node(signature))
            else:
                newNode = Node(signature)
                s3Graph[newNode] = set()
                for node in s3Graph.keys():
                    if node.signature.issubset(newNode.signature):
                        s3Graph[node].add(newNode)
        for typeName in C:
            if Node(typeName) not in markedNodes:
                deleteTypeInS3Graph(typeName)


def filterNonObjects():
    deletedNodes = []
    for iNode in s3Graph.keys():
        for jNode in s3Graph.keys():
            if (not (iNode.signature & jNode.signature)) and Node(iNode.signature | jNode.signature) in s3Graph.keys():
                deletedNodes.append(Node(iNode.signature | jNode.signature))

    for deleteNode in deletedNodes:
        deleteNodeInS3Graph(deleteNode)

    for node in s3Graph.keys():
        if node in s3Graph[node]:
            s3Graph[node].remove(node)


def constructS3Graph(events):
    # for i in range(len(events)):
    #     updated = TypeRelationGraph.updateTypeRelationGraph(events[i])
    #     if updated:
    #         typeRelationGraph = TypeRelationGraph.typeRelationGraph
    #         initS3GraphFromTypeRelationGraph(TypeRelationGraph.typeRelationGraph)
    #         merge11Nodes(typeRelationGraph, events[0:i + 1])
    #         mergeMNNodes(typeRelationGraph, events[0:i + 1])
    #         filterNonObjects()
    #
    # return s3Graph
    typeRelationGraph = TypeRelationGraph.getTypeRelationGraph(events)
    initS3GraphFromTypeRelationGraph(typeRelationGraph)
    # 3 steps to construct S3Graph
    merge11Nodes(typeRelationGraph, events)
    mergeMNNodes(typeRelationGraph, events)
    filterNonObjects()
    return s3Graph


if __name__ == '__main__':
    print constructS3Graph(CommonUtils.getEvents())
