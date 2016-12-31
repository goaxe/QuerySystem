

def getTypeList(events):
    typeSet = set(typeName for event in events for typeName in event.keys())
    return list(typeSet)


def getTypePairListGraph(events):
    typeList = getTypeList(events)
    typeSize = len(typeList)
    typeToIndex = {}
    for i in range(typeSize):
        typeToIndex[typeList[i]] = i

    typePairListGraph = {}
    for typeName in typeList:
        typePairListGraph[typeName] = {}

    for event in events:
        itemList = event.items()
        for i in itemList:
            for j in itemList:
                if i == j:
                    continue
                iName, iValue = i
                jName, jValue = j
                if iName not in typePairListGraph[jName]:
                    typePairListGraph[jName][iName] = {}
                if jName not in typePairListGraph[iName]:
                    typePairListGraph[iName][jName] = {}
                if iValue not in typePairListGraph[iName][jName]:
                    typePairListGraph[iName][jName][iValue] = set()
                if jValue not in typePairListGraph[jName][iName]:
                    typePairListGraph[jName][iName][jValue] = set()
                typePairListGraph[iName][jName][iValue].add(jValue)
                typePairListGraph[jName][iName][jValue].add(iValue)

    return typePairListGraph


def getTypeRelation(ijDict, jiDict):
    ijIs11 = jiIs11 = True
    for s in ijDict.values():
        if len(s) > 1:
            ijIs11 = False
            break
    for s in jiDict.values():
        if len(s) > 1:
            jiIs11 = False
            break
    return ijIs11, jiIs11


# 1->1:1  2->1:n  3:m:n   -2->n:1
def getTypeRelationGraph(events):
    typeList = getTypeList(events)
    typePairListGraph = getTypePairListGraph(events)

    typeRelationGraph = {}
    for type in typeList:
        typeRelationGraph[type] = {}

    for iType in typeList:
        for jType in typeList:
            if iType == jType:
                continue
            if jType not in typePairListGraph[iType]:
                continue
            ijDict = typePairListGraph[iType][jType]
            jiDict = typePairListGraph[jType][iType]

            ijIs11, jiIs11 = getTypeRelation(ijDict, jiDict)

            if ijIs11 and jiIs11:
                typeRelationGraph[iType][jType] = 1
                typeRelationGraph[jType][iType] = 1
            elif ijIs11 and (not jiIs11):
                typeRelationGraph[iType][jType] = -2
                typeRelationGraph[jType][iType] = 2
            elif (not ijIs11) and jiIs11:
                typeRelationGraph[iType][jType] = 2
                typeRelationGraph[jType][iType] = -2
            else:
                typeRelationGraph[iType][jType] = 3
                typeRelationGraph[jType][iType] = 3

    return typeRelationGraph
