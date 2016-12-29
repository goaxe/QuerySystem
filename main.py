import json
from itertools import combinations

JOIN_CHAR = '*'


def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


def getEvents():
    f = open("data/events.data")
    events = []
    while 1:
        line = f.readline()
        if not line:
            break
        try:
            events.append(byteify(json.loads(line.encode('utf8'))))
        except Exception as e:
            print e
    f.close()
    print events
    return events


def getAllTypes(events):
    typeSet = set()
    for event in events:
        typeSet.update(event.keys())

    return ['USER_ID', 'USER', 'QUERY', 'APP', 'ATTEMPT_M', 'ATTEMPT_R', 'CONTAINER', 'FETCHER']
    # return list(typeSet)


def getTypePairListGraph(events):
    typeList = getAllTypes(events)
    typeSize = len(typeList)
    typeToIndex = {}
    for i in range(typeSize):
        typeToIndex[typeList[i]] = i

    typePairListGraph = [[[] for col in range(typeSize)] for row in range(typeSize)]
    for event in events:
        itemList = event.items()
        itemSize = len(itemList)
        for i in range(itemSize):
            for j in range(i + 1, itemSize):
                iIndex = typeToIndex[itemList[i][0]]
                jIndex = typeToIndex[itemList[j][0]]
                typePairListGraph[iIndex][jIndex].append((itemList[i][1], itemList[j][1]))
                typePairListGraph[jIndex][iIndex].append((itemList[j][1], itemList[i][1]))

    return typePairListGraph


def getTypeRelation(ijList, jiList):
    ijDict = {}
    jiDict = {}
    ijIs11 = True
    jiIs11 = True
    for item in ijList:
        if item[0] not in ijDict:
            ijDict[item[0]] = set()
        ijDict[item[0]].add(item[1])
    for v in ijDict.values():
        if len(v) > 1:
            ijIs11 = False
            break

    for item in jiList:
        if item[0] not in jiDict:
            jiDict[item[0]] = set()
        jiDict[item[0]].add(item[1])
    for v in jiDict.values():
        if len(v) > 1:
            jiIs11 = False
            break

    return ijIs11, jiIs11


# 0->empty  1->1:1  2->1:n  3:m:n   -2->n:1
def getTypeRelationGraph(events):
    typeList = getAllTypes(events)
    typeSize = len(typeList)
    typePairListGraph = getTypePairListGraph(events)

    # typeRelationGraph = [[0 for col in range(typeSize)] for row in range(typeSize)]
    typeRelationGraph = {}
    for type in typeList:
        typeRelationGraph[type] = {}

    for i in range(typeSize):
        for j in range(i, typeSize):
            if i == j:
                typeRelationGraph[typeList[i]][typeList[i]] = 1
                continue
            ijList = typePairListGraph[i][j]
            jiList = typePairListGraph[j][i]
            if not ijList and not jiList:
                typeRelationGraph[typeList[i]][typeList[j]] = 0
                typeRelationGraph[typeList[j]][typeList[i]] = 0
                continue

            ijIs11, jiIs11 = getTypeRelation(ijList, jiList)

            # if typeList[i] == 'USER' and typeList[j] == 'QUERY':
            #     print '==========='
            #     print ijDict
            #     print jiDict
            #     print ijIs11, jiIs11

            if ijIs11 and jiIs11:
                typeRelationGraph[typeList[i]][typeList[j]] = 1
                typeRelationGraph[typeList[j]][typeList[i]] = 1
            elif ijIs11 and (not jiIs11):
                typeRelationGraph[typeList[i]][typeList[j]] = -2
                typeRelationGraph[typeList[j]][typeList[i]] = 2
            elif (not ijIs11) and jiIs11:
                typeRelationGraph[typeList[i]][typeList[j]] = 2
                typeRelationGraph[typeList[j]][typeList[i]] = -2
            else:
                typeRelationGraph[typeList[i]][typeList[j]] = 3
                typeRelationGraph[typeList[j]][typeList[i]] = 3

    return typeList, typeRelationGraph


def get11Subsets(typeList, typeRelationGraph):
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
                if jType != first and typeRelationGraph[first][jType] == 1 and jType not in iSet:
                    iSet.add(jType)
                    queue.append(jType)

    return subsets


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
                if jType != first and typeRelationGraph[first][jType] == 3 and jType not in iSet:
                    ismn = True
                    for item in iSet:
                        if typeRelationGraph[item][jType] != 3:
                            ismn = False
                            break
                    if ismn:
                        iSet.add(jType)
                        queue.append(jType)
    return subsets


def merge11Nodes(typeList, typeRelationGraph, events):
    subsets = get11Subsets(typeList, typeRelationGraph)
    for subset in subsets:
        if len(subset) == 1:
            continue

        typeNames = subset
        mergedTypeName = JOIN_CHAR.join(typeNames)

        kvList = []

        for event in events:
            kvs = {}
            for key in event.keys():
                if key in typeNames:
                    kvs[key] = event[key]
            if len(kvs.keys()) == 0:
                continue

            hasExist = False
            for item in kvList:
                for (k, v) in kvs.items():
                    if k in item and item[k] == v:
                        item.update(kvs)
                        hasExist = True
                        break
                if hasExist:
                    break
            if not hasExist:
                kvList.append(kvs)

        for event in events:
            hasExist = False

            k, v = '', ''

            for key in event.keys():
                if key in typeNames:
                    k, v = key, event[key]
                    del event[key]
                    hasExist = True
            if hasExist:
                for kv in kvList:
                    if kv[k] == v:
                        event[mergedTypeName] = '*'.join(kv.values())
        typeRelationGraph[mergedTypeName] = {}
        for typeName in typeNames:
            for k, v in typeRelationGraph[typeName].items():
                typeRelationGraph[mergedTypeName][k] = v
            del typeRelationGraph[typeName]
        for remainType in typeRelationGraph:
            existType = False
            typeRelation = -1
            for delType in typeNames:
                if delType in typeRelationGraph[remainType]:
                    typeRelation = typeRelationGraph[remainType][delType]
                    existType = True
            if existType:
                typeRelationGraph[remainType][mergedTypeName] = typeRelation

    for event in events:
        print event
    print 'typeRelationGraph'
    for k, v in typeRelationGraph.items():
        print k, v
    print '========================'


def getCombination(subset):
    result = []
    for i in range(2, len(subset) + 1):
        result.extend(combinations(subset, i))
    return result


def deleteTypeInGraph(typeRelationGraph, delType):
    if delType in typeRelationGraph:
        del typeRelationGraph[delType]
    for typeName in typeRelationGraph.keys():
        if delType in typeRelationGraph[typeName]:
            del typeRelationGraph[typeName][delType]


def mergemnNodes(typeRelationGraph, events):
    subsets = getmnSubsets(typeRelationGraph)

    for subset in subsets:
        if len(subset) == 1:
            continue
        C = subset
        markedNodes = set()
        newNodes = set()
        for event in events:
            signature = C & set(event.keys())
            if not signature:
                continue
            exist = False
            for node in typeRelationGraph.keys():
                if set(node.split(JOIN_CHAR)) == signature:
                    exist = True
            if exist:
                markedNodes.add(JOIN_CHAR.join(signature))
            else:
                newNodes.add(JOIN_CHAR.join(signature))
        print 'C:', C
        print 'markedNodes:', markedNodes
        print 'newNodes:', newNodes
        for typeName in C:
            if typeName not in markedNodes:
                deleteTypeInGraph(typeRelationGraph, typeName)
        for newNode in newNodes:
            typeRelationGraph[newNode] = {}


def filterNonObjects(typeRelationGraph):
    typeNames = typeRelationGraph.keys()
    deleteSigs = []
    signatureSet = []
    for typeName in typeNames:
        sig = set()
        sig.update(typeName.split(JOIN_CHAR))
        signatureSet.append(sig)
    for iSig in signatureSet:
        for jSig in signatureSet:
            if iSig == jSig:
                continue
            if (not (iSig & jSig)) and ((iSig | jSig) in signatureSet):
                print iSig, jSig
                deleteSigs.append(iSig | jSig)
    for typeName in typeNames:
        if set(typeName.split(JOIN_CHAR)) in deleteSigs:
            deleteTypeInGraph(typeRelationGraph, typeName)
    for (k, v) in typeRelationGraph.items():
        print k, v


def constructS3Graph():
    events = getEvents()
    typeList, typeRelationGraph = getTypeRelationGraph(events)
    merge11Nodes(typeList, typeRelationGraph, events)
    mergemnNodes(typeRelationGraph, events)
    filterNonObjects(typeRelationGraph)


if __name__ == '__main__':
    constructS3Graph()

