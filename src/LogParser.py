# encoding: UTF-8

import re
import time
import datetime


def parse(line):
    regexList = [
        'appid=[a-z]+_[0-9]+_[0-9]+',
        'user=[a-z]+',
        'containerid=[a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'container=[a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'container [a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'appattemptid=[a-z]+_[0-9]+_[0-9]+_[0-9]+',
        'attempt=[a-z]+_[0-9]+_[0-9]+_[0-9]+',
        'attempt [a-z]+_[0-9]+_[0-9]+_[0-9]+'
    ]
    timeRegex = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'

    log = dict()
    m = re.search(timeRegex, line)
    if m:
        log['timeline'] = time.mktime(datetime.datetime.strptime(str(m.group()), '%Y-%m-%d %H:%M:%S').timetuple())
    else:
        return log

    event = dict()
    for regex in regexList:
        m = re.search(regex, line, re.I)
        if m:
            pair = re.split('=| ', str(m.group()))
            if len(pair) != 2:
                print '===========Unexcepted: ', pair
                continue
            key, value = pair[0].lower(), pair[1].lower()
            if 'attempt' in key:
                event['attempt'] = value
            elif 'container' in key:
                event['container'] = value
            else:
                event[key] = value
    log['event'] = event
    return log


def parseLog():
    logs = []
    f = open('./data/yarn-root-resourcemanager-tfs01.log')
    while True:
        line = f.readline()
        if not line:
            break
        log = parse(line)
        if log and log['event']:
            logs.append(log)
    f.close()
    f = open('./data/parser.data', 'w')
    for log in logs:
        f.write(str(log) + '\n')
    f.close()

    return logs


if __name__ == '__main__':
    parseLog()
