# encoding=utf-8
import json
import LogParser

JOIN_CHAR = '*'


def byteify(inData):
    if isinstance(inData, dict):
        return {byteify(key): byteify(value)
                for key, value in inData.iteritems()}
    elif isinstance(inData, list):
        return [byteify(element) for element in inData]
    elif isinstance(inData, unicode):
        return inData.encode('utf-8')
    else:
        return inData


def getEvents():
    return LogParser.parseLog()

    # f = open("data/events.data")
    # events = []
    # while 1:
    #     line = f.readline()
    #     if not line:
    #         break
    #     try:
    #         events.append(byteify(json.loads(line.encode('utf8'))))
    #     except Exception as e:
    #         print e
    # f.close()
    # return events
