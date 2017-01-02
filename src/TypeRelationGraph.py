typePairListGraph = dict()
typeRelationGraph = dict()


def updateTypePairListGraph(event):
    itemList = event.items()
    for i in itemList:
        for j in itemList:
            if i == j:
                continue
            iName, iValue = i
            jName, jValue = j
            if iName not in typePairListGraph:
                typePairListGraph[iName] = dict()
            if jName not in typePairListGraph:
                typePairListGraph[jName] = dict()
            if iName not in typePairListGraph[jName]:
                typePairListGraph[jName][iName] = dict()
            if jName not in typePairListGraph[iName]:
                typePairListGraph[iName][jName] = dict()
            if iValue not in typePairListGraph[iName][jName]:
                typePairListGraph[iName][jName][iValue] = set()
            if jValue not in typePairListGraph[jName][iName]:
                typePairListGraph[jName][iName][jValue] = set()
            typePairListGraph[iName][jName][iValue].add(jValue)
            typePairListGraph[jName][iName][jValue].add(iValue)


def getTypePairListGraph(events):
    for event in events:
        updateTypePairListGraph(typePairListGraph, event)

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


def updateTypeRelationGraph(event):
    updateTypePairListGraph(event)
    candidateTypes = event.keys()
    typeRelationGraphUpdated = False
    for iType in candidateTypes:
        for jType in candidateTypes:
            if iType == jType:
                continue
            if iType not in typeRelationGraph:
                typeRelationGraph[iType] = dict()
                typeRelationGraphUpdated = True
            if jType not in typeRelationGraph:
                typeRelationGraph[jType] = dict()
                typeRelationGraphUpdated = True

            ijDict = typePairListGraph[iType][jType]
            jiDict = typePairListGraph[jType][iType]
            ijIs11, jiIs11 = getTypeRelation(ijDict, jiDict)

            if ijIs11 and jiIs11:
                if jType not in typeRelationGraph[iType] or typeRelationGraph[iType][jType] != 1:
                    typeRelationGraph[iType][jType] = 1
                    typeRelationGraph[jType][iType] = 1
                    typeRelationGraphUpdated = True
            elif ijIs11 and (not jiIs11):
                if jType not in typeRelationGraph[iType] or typeRelationGraph[iType][jType] != -2:
                    typeRelationGraph[iType][jType] = -2
                    typeRelationGraph[jType][iType] = 2
                    typeRelationGraphUpdated = True
            elif (not ijIs11) and jiIs11:
                if jType not in typeRelationGraph[iType] or typeRelationGraph[iType][jType] != 2:
                    typeRelationGraph[iType][jType] = 2
                    typeRelationGraph[jType][iType] = -2
                    typeRelationGraphUpdated = True
            else:
                if jType not in typeRelationGraph[iType] or typeRelationGraph[iType][jType] != 3:
                    typeRelationGraph[iType][jType] = 3
                    typeRelationGraph[jType][iType] = 3
                    typeRelationGraphUpdated = True

    return typeRelationGraphUpdated


# 1->1:1  2->1:n  3:m:n   -2->n:1
def getTypeRelationGraph(events):
    for event in events:
        updateTypeRelationGraph(event)

    return typeRelationGraph
