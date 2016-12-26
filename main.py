import json

DEBUG = True


def getEvents():
    f = open("data/events.data")
    events = []
    while 1:
        line = f.readline()
        if not line:
            break
        try:
            events.append(json.loads(line))
        except Exception as e:
            if DEBUG:
                print line
                print e
    f.close()
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

    typeRelationGraph = [[0 for col in range(typeSize)] for row in range(typeSize)]

    for i in range(typeSize):
        for j in range(i, typeSize):
            if i == j:
                typeRelationGraph[i][j] = 1
                continue
            ijList = typePairListGraph[i][j]
            jiList = typePairListGraph[j][i]
            if not ijList and not jiList:
                typeRelationGraph[i][j] = 0
                typeRelationGraph[j][i] = 0
                continue

            ijIs11, jiIs11 = getTypeRelation(ijList, jiList)

            # if typeList[i] == 'USER' and typeList[j] == 'QUERY':
            #     print '==========='
            #     print ijDict
            #     print jiDict
            #     print ijIs11, jiIs11

            if ijIs11 and jiIs11:
                typeRelationGraph[i][j] = 1
                typeRelationGraph[j][i] = 1
            elif ijIs11 and (not jiIs11):
                typeRelationGraph[i][j] = -2
                typeRelationGraph[j][i] = 2
            elif (not ijIs11) and jiIs11:
                typeRelationGraph[i][j] = 2
                typeRelationGraph[j][i] = -2
            else:
                typeRelationGraph[i][j] = 3
                typeRelationGraph[j][i] = 3

    if DEBUG:
        print typeList
        for item in typeRelationGraph:
            print item
    return typeList, typeRelationGraph


def getSubsets(typeList, typeRelationGraph):
    subsets = []

    typeSize = len(typeList)
    for i in range(typeSize):
        hasExist = False
        iSet = []
        for subset in subsets:
            if i in subset:
                hasExist = True
                iSet = subset
        if not hasExist:
            iSet = set()
            iSet.add(i)
            subsets.append(iSet)
        queue = [i]
        while len(queue):
            first = queue[0]
            queue.pop()
            for j in range(typeSize):
                if j != first and typeRelationGraph[first][j] == 1 and j not in iSet:
                    iSet.add(j)
                    queue.append(j)

    return subsets


def merge11Nodes(typeList, typeRelationGraph, events):
    subsets = getSubsets(typeList, typeRelationGraph)
    for subset in subsets:
        if len(subset) == 1:
            continue

        typeNames = [typeList[index] for index in subset]
        mergedTypeName = '*'.join(typeList[index] for index in subset)
        if DEBUG:
            print mergedTypeName

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
                if DEBUG:
                    print 'kvs: ', kvs, len(kvs)
                kvList.append(kvs)

        if DEBUG:
            print 'kvList:', kvList

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
                        if DEBUG:
                            print 'event:', event
                        break

def main():
    events = getEvents()
    typeList, typeRelationGraph = getTypeRelationGraph(events)
    print getSubsets(typeList, typeRelationGraph)
    merge11Nodes(typeList, typeRelationGraph, events)


if __name__ == '__main__':
    main()

