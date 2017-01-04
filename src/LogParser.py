# encoding: UTF-8

import re


def parse(line):
    regexList = [
        'appid=[a-z]+_[0-9]+_[0-9]+',
        'user=[a-z]+',
        # 'ip=[0-9]+.[0-9]+.[0-9]+.[0-9]+',
        'containerid=[a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'container=[a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'container [a-z]+_[0-9]+_[0-9]+_[0-9]+_[0-9]+',
        'appattemptid=[a-z]+_[0-9]+_[0-9]+_[0-9]+',
        'attempt=[a-z]+_[0-9]+_[0-9]+_[0-9]+',
        'attempt [a-z]+_[0-9]+_[0-9]+_[0-9]+'
    ]
    timeRegex = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}'

    log = dict()
    m = re.search(timeRegex, line)
    if m:
        log['timeline'] = str(m.group())
    else:
        print 'not match'
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
    events = [log['event'] for log in logs]


    return events


if __name__ == '__main__':
    parseLog()
