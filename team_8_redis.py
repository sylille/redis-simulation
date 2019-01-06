#-*- coding: utf-8 -*-
from rediscluster import StrictRedisCluster
#from rediscluster import StrictRedisCluster
import re
import pandas as pd


startup_nodes = [{"host": "127.0.0.1", "port": "7000"},{"host": "127.0.0.1", "port": "7001"},{"host": "127.0.0.1", "port": "7002"}]#{"host": "127.0.0.1", "port": "7003"},{"host": "127.0.0.1", "port": "7004"},{"host": "127.0.0.1", "port": "7005"}]
r =  StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)  # type: StrictRedisCluster

def hello_redis():
    print("Welcome DB with NoSQL")
    while True:
        try:
            query = raw_input("Program Query Input > ")
            # print(array)
            # Basic functions (Let's get 60%!!!!!!!)
            # CREATE TABLE (table name)
            if (query.split(" ")[0].upper() == "CREATE"):
                create(r, query)
            elif (query.split(" ")[0].upper() == "INSERT"):
                insert(r, query)
            # redis done
            elif (query.split(" ")[0].upper() == "SELECT"):
                select(query)
            elif (query.split(" ")[0].upper() == "UPDATE"):
                update(query)
            elif (query.split(" ")[0].upper() == "DELETE"):
                delete(query)
            elif (query.split(" ")[0].upper() == "SHOW"):
                show()
            elif (query.split(" ")[0].upper() == "FLUSHALL"):
                flush()
            elif (query.split(" ")[0].upper() == "FLUSHALL;"):
                flush()
            elif (query.split(" ")[0].upper() == "EXEC"):
                exec (query.split(" ")[1])
            else:
                print("WARNING: Inappropriate Query!")
                raise NotImplementedError

        except Exception as e:
            print(e)


def create(r, query):
    # Check if the first word is 'create' and the second is 'table'

    # ******* Parsing part *******
    # output : tableName, source (att1, val1, att2, val2...)
    # Check if the first word is 'create' and the second is 'table'
    pattern0 = 'table '
    query = query.replace(';', '')
    t = re.compile(pattern0, re.IGNORECASE)
    matchTable = t.search(query)
    realQuery = query[matchTable.end():]
    tableName = realQuery.split('(')[0]
    tableName = tableName.replace(' ', '')
    openingParen = realQuery.find('(')
    realQuery = realQuery[openingParen + 1:-1]
    columnList = realQuery.split(',')
    source = []
    for i in columnList:
        temp = i.split()
        for j in temp:
            source.append(j)

    # print(columnList)
    # print(source)

    # ******* Redis part *******
    strIn = "".join("r.hmset (\"" + "META:" + tableName + "\", {")
    num = 0;
    atts = ""
    for i in range(0, len(source)):
        if i % 2 == 0:
            if atts is not "":
                atts = atts + ":"
            # column name saving
            num = num + 1
            strIn = strIn + " \"" + str(source[i]) + "\":\"" + str(source[i + 1]) + "\","
            atts = atts + str(source[i])
    r.set(tableName, atts)
    # print(r.get(tableName))
    strIn = strIn + "\"size\":0})"
    strln = strIn + "})"
    # r.hmset ("META:T1", { "make":"int", "model":"int","size":0})
    # r.hmset ("META:T1", { "make":"int", "model":"int","size":0})
    # print(strIn)
    exec (strIn)
    # add to set tables (add "TABLE:")
    tableName = "TABLE:" + tableName
    r.sadd("Tables", tableName)
    show()


def insert(r, query):
    # Check if the first word is 'create' and the second is 'table'

    # ******* Parsing part *******
    query = query.replace(';', '')
    array = query.split()
    if (array[0].lower() != 'insert') or (array[1].lower() != 'into'):
        raise NotImplementedError
    else:
        query = query.replace("\"", "\'")
        tableName = ''.join((array[2]))
        query = query.split('(')[1]
        query = query.replace(')', '')
        query = query.replace(' ', '')
        source = query.split(',')

    # ******* Redis part *******
    # T1 / (123, 458) => ['123','458'] / ('123','458') => ["'123'","'458'"]
    # Making a hash 'tableName:rowNum' (hmset)
    hashKeys = r.hkeys("META:" + tableName)
    currentSize = int(r.hget("META:" + tableName, "size"))
    rowNum = currentSize + 1
    strIn = "".join("r.hmset (\"" + tableName + ":" + str(rowNum) + "\", {")
    metaLen = r.hlen("META:" + tableName)

    # print(r.hgetall("META:"+tableName))
    # print(hashKeys)
    # print(r.get(tableName))
    # print(source)

    attributes = r.get(tableName).split(":")
    # print(attributes)

    for i in range(0, len(attributes)):
        strIn = strIn + " \"" + str(attributes[i]) + "\":\"" + str(source[i]) + "\","
        setName = str(attributes[i]) + ":" + str(source[i])
        saddQuery = "r.sadd(\"" + setName + "\",\"" + tableName + ":" + str(rowNum) + "\")"
        # print(saddQuery)
        exec (saddQuery)
        # r.sadd("make:123",1), r.sadd("model:458",1)
        # if type is integer
        tp = r.hget("META:" + tableName, attributes[i])

        # int일 때 zadd 만들어주기
        if (("int" in tp) or ("INT" in tp)):
            # zadd(sortedSetName,name,score})
            # print("this column is an int!")
            zaddQuery = "r.zadd(\"" + tableName + ":" + attributes[i] + "\",\"" + str(source[i]) + "\"," + str(
                rowNum) + ")"
            # print(zaddQuery)
            exec (zaddQuery)
        else:

            # char일 때 sadd에 이름 넣어주기
            # print("this column is not an int, but a character!")
            # print(setName+" in a Sets set")
            r.sadd("Sets", setName)
    strln = strIn + "})"
    # print(strln)
    exec (strln)
    # r.hmset ("T1:1", { "make":"123", "model":"458",})
    # Updating metadata (size)
    updateQuery = "r.hset(\"META:" + tableName + "\",\"size\"," + str(rowNum) + ")"
    # print(updateQuery)
    exec (updateQuery)
    # exec('r.hmset (\"'+tableName+':'+id+',{\"'+make+'\":'+123', '+model+':'+458+'})')
    print("INSERT SUCCESS : TABLE " + tableName + " has " + str(rowNum) + " rows.")


def select(query):
    # output : selecting column, tableName, subquery for where clause, pattern for like matching

    # ******* Parsing Part *******

    # Step 1. Preprocessing
    query = query.replace(';', '')
    # it does not fliter '(' and ')'. they will be handled separately. To filter them, user the codes below.
    pattern0 = 'select '
    pattern1 = 'from '
    pattern2 = 'where '
    s = re.compile(pattern0, re.IGNORECASE)
    f = re.compile(pattern1, re.IGNORECASE)
    w = re.compile(pattern2, re.IGNORECASE)
    matchSelect = s.search(query)
    matchFrom = f.search(query)
    matchWhere = w.search(query)
    column = query[matchSelect.end():matchFrom.start()]
    column = column.replace(" ", "")
    columnList = column.split(",")
    whereQuery = ""
    if matchWhere is not None:
        # If there's WHERE
        tableName = query[matchFrom.end():matchWhere.start()]
        whereQuery = query[matchWhere.end():]
    else:
        tableName = query[matchFrom.end():]
    # print(columnList)
    # print(tableName)
    # print(whereQuery)

    # ******* Redis Part ********

    # Step 2. if columnList is *, update columnList as all columns
    if columnList[0] == '*':
        metaKey = 'META:' + tableName
        allKeys = r.hkeys(metaKey)
        # print(allKeys)
        columnList = []
        for i in allKeys:
            if 'size' not in i:
                columnList.append(i)

    if matchWhere == None:
        # Step 3. Picking data based on column names.
        # So we have columnList, tableName and whereQuery.
        columnDic = {}
        for i in columnList:
            # i = columnName
            # if colomn i has int
            # print(i)
            tp = r.hget('META:' + tableName, i)
            if ('int' in tp) or ('INT' in tp):
                dataLst = pickingZ(tableName, i)
            else:
                dataLst = pickingS(tableName, i)
            columnDic[i] = dataLst
            # print(dataLst)
            # print(columnDic)
        makingDictColumn(columnDic)
    else:
        # WHERE 처리단
        tableName = tableName[0:-1]
        # Step 4. Filtering using whereQuery
        if '(' in whereQuery:
            # 괄호가 있을 때
            print("( implementation needed!")
        else:
            # 한 괄호 안에서
            if ('and' in whereQuery) and ('or' not in whereQuery):
                # only AND
                eachQueries = whereQuery.split(' and ')
                a = eachQueries[0]
                b = eachQueries[1]
                firstList = singleQuery(tableName, a)
                secondList = singleQuery(tableName, b)
                addlst = []
                for i in firstList:
                    for j in secondList:
                        if (i == j) and (i not in addlst) and (j not in addlst):
                            addlst.append(i)
            elif ('and' not in whereQuery) and ('or' in whereQuery):
                # only OR
                eachQueries = whereQuery.split(' or ')
                a = eachQueries[0]
                b = eachQueries[1]
                firstList = singleQuery(tableName, a)
                secondList = singleQuery(tableName, b)
                addlst = []
                for i in secondList:
                    if i not in firstList:
                        addlst.append(i)
                addlst = addlst + firstList
                for i in secondList:
                    for j in addlst:
                        if (i != j) and (i not in addlst):
                            addlst.append(i)
            elif ('and' in whereQuery) and ('or' in whereQuery):
                # AND and OR
                # if there is no ( ) in where query
                addlst = []
                print("and & or not implemented!")
            else:
                addlst = singleQuery(tableName, whereQuery)
        addToTable(addlst, columnList)


# WHERE에 관련한 헬퍼메소드들

def singleQuery(tableName, whereQuery):
    if ('=' in whereQuery) and ('>' not in whereQuery) and ('<' not in whereQuery):
        addlst = equals(tableName, whereQuery)
    elif ('<' in whereQuery) and ('>' not in whereQuery) and ('=' not in whereQuery):
        addlst = smaller(tableName, whereQuery)
    elif ('>' in whereQuery) and ('<' not in whereQuery) and ('=' not in whereQuery):
        addlst = larger(tableName, whereQuery)
    elif ('<' in whereQuery) and ('>' not in whereQuery) and ('=' in whereQuery):
        addlst = equalOrSmaller(tableName, whereQuery)
    elif ('>' in whereQuery) and ('<' not in whereQuery) and ('=' in whereQuery):
        addlst = equalOrLarger(tableName, whereQuery)
        # addToTable(addlst, columnList)
    elif ('!=' in whereQuery) and ('>' not in whereQuery) and ('<' not in whereQuery):
        print("!= not yet implemented!")
    return addlst


def addToTable(addlst, columnlst):
    # 주소값으로 row를 불러온다
    # row에서 원하는 column을 걸러 딕셔너리를 만든다 (이미 table에서는 걸러져있지..?)
    # 딕셔너리를 pandas로 표로 만든다
    columnDic = {}
    for i in columnlst:
        columnValues = []
        for j in addlst:
            columnValues.append(r.hget(j, i))
        # print(columnValues)
        columnDic[i] = columnValues
    makingDictColumn(columnDic)


def equals(tableName, whereQuery):
    # output : a list of row adress (e.i.) set([u'T2:1'])
    whereQuery = whereQuery.replace(" = ", "=")
    whereQuery = whereQuery.replace(" =", "=")
    whereQuery = whereQuery.replace("= ", "=")
    column = whereQuery.split("=")[0]
    value = whereQuery.split("=")[1]
    columnList = r.get(tableName)
    columnList = columnList.split(":")

    for i in columnList:
        if i in column:
            realName = i
    column = realName

    keysForType = r.hkeys("META:" + tableName)
    for k in keysForType:
        if column in k:
            tp = r.hget("META:" + tableName, column)

    # 우리가 가지고 있는 것 : column, tableName
    addressLst = []
    if ((tp == 'int') or (tp == 'INT')):
        # When it is INT
        zName = tableName + ":" + column
        # print(zName)
        value = int(value)
        idLst = r.zrangebyscore(zName, value, value)
        # print(idLst)
        for id in idLst:
            rowAddress = tableName + ":" + id
            addressLst.append(rowAddress)
    else:
        # When it is VARCHAR
        sName = column + ":" + value
        # print(sName)
        addressLst = r.smembers(column + ":" + value)
    return addressLst


def smaller(tableName, whereQuery):
    # output : a list of row adress (e.i.) set([u'T2:1'])
    whereQuery = whereQuery.replace(" < ", "<")
    whereQuery = whereQuery.replace(" <", "<")
    whereQuery = whereQuery.replace("< ", "<")
    column = whereQuery.split("<")[0]
    value = whereQuery.split("<")[1]
    columnList = r.get(tableName)
    columnList = columnList.split(":")

    for i in columnList:
        if i in column:
            realName = i
    column = realName

    keysForType = r.hkeys("META:" + tableName)
    for k in keysForType:
        if column in k:
            tp = r.hget("META:" + tableName, column)

    # 우리가 가지고 있는 것 : column, tableName
    addressLst = []
    if ((tp == 'int') or (tp == 'INT')):
        # When it is INT
        zName = tableName + ":" + column
        # print(zName)
        value = int(value)
        # 제시된 밸류 자체는 미포함
        idLst = r.zrangebyscore(zName, float('-inf'), value - 1)
        # print(idLst)
        for id in idLst:
            rowAddress = tableName + ":" + id
            addressLst.append(rowAddress)
    return addressLst


def larger(tableName, whereQuery):
    # output : a list of row adress (e.i.) set([u'T2:1'])
    whereQuery = whereQuery.replace(" > ", ">")
    whereQuery = whereQuery.replace(" >", ">")
    whereQuery = whereQuery.replace("> ", ">")
    column = whereQuery.split(">")[0]
    value = whereQuery.split(">")[1]
    columnList = r.get(tableName)
    columnList = columnList.split(":")

    for i in columnList:
        if i in column:
            realName = i
    column = realName

    keysForType = r.hkeys("META:" + tableName)
    for k in keysForType:
        if column in k:
            tp = r.hget("META:" + tableName, column)

    # 우리가 가지고 있는 것 : column, tableName
    addressLst = []
    if ((tp == 'int') or (tp == 'INT')):
        # When it is INT
        zName = tableName + ":" + column
        # print(zName)
        value = int(value)
        # 제시된 밸류 자체는 미포함
        idLst = r.zrangebyscore(zName, value + 1, float('inf'))
        # print(idLst)
        for id in idLst:
            rowAddress = tableName + ":" + id
            addressLst.append(rowAddress)
    return addressLst


def equalOrSmaller(tableName, whereQuery):
    # output : a list of row adress (e.i.) set([u'T2:1'])
    whereQuery = whereQuery.replace(" <= ", "<=")
    whereQuery = whereQuery.replace(" <=", "<=")
    whereQuery = whereQuery.replace("<= ", "<=")
    column = whereQuery.split("<=")[0]
    value = whereQuery.split("<=")[1]
    columnList = r.get(tableName)
    columnList = columnList.split(":")

    for i in columnList:
        if i in column:
            realName = i
    column = realName

    keysForType = r.hkeys("META:" + tableName)
    for k in keysForType:
        if column in k:
            tp = r.hget("META:" + tableName, column)

    # 우리가 가지고 있는 것 : column, tableName
    addressLst = []
    if ((tp == 'int') or (tp == 'INT')):
        # When it is INT
        zName = tableName + ":" + column
        # print(zName)
        value = int(value)
        # 제시된 밸류 자체는 미포함
        idLst = r.zrangebyscore(zName, float('-inf'), value)
        # print(idLst)
        for id in idLst:
            rowAddress = tableName + ":" + id
            addressLst.append(rowAddress)
    return addressLst


def equalOrLarger(tableName, whereQuery):
    # output : a list of row adress (e.i.) set([u'T2:1'])
    whereQuery = whereQuery.replace(" >= ", ">=")
    whereQuery = whereQuery.replace(" >=", ">=")
    whereQuery = whereQuery.replace(">= ", ">=")
    column = whereQuery.split(">=")[0]
    value = whereQuery.split(">=")[1]
    columnList = r.get(tableName)
    columnList = columnList.split(":")

    for i in columnList:
        if i in column:
            realName = i
    column = realName

    keysForType = r.hkeys("META:" + tableName)
    for k in keysForType:
        if column in k:
            tp = r.hget("META:" + tableName, column)

    # 우리가 가지고 있는 것 : column, tableName
    addressLst = []
    if ((tp == 'int') or (tp == 'INT')):
        # When it is INT
        zName = tableName + ":" + column
        # print(zName)
        value = int(value)
        # 제시된 밸류 자체는 미포함
        idLst = r.zrangebyscore(zName, value, float('inf'))
        # print(idLst)
        for id in idLst:
            rowAddress = tableName + ":" + id
            addressLst.append(rowAddress)
    return addressLst


def makingDictColumn(columnDic):
    df = pd.DataFrame.from_dict(columnDic)
    print(df)


# WHERE 없을 때 헬퍼메소드들

def pickingZ(tableName, columnName):
    # input : a column name
    # output : a list of all the data in a column
    # the output SHOULD not be ordered
    zName = tableName + ":" + columnName
    # Process for getting a table size (it will be number of values (ids))
    metaKey = ('META:' + tableName)
    # Pick scores
    ssName = tableName + ':' + columnName
    # zRange쓰면 자동정렬되어 나온다. 딕셔너리 -> 리스트를 통해 아이디 순서대로 정렬해준다.
    # print(ssName)
    allElements = r.zrange(ssName, 0, -1, desc=False, withscores=True)
    # print("all")
    # print(allElements)
    dataLst = []
    orderDict = {}
    for i in allElements:
        orderDict[i[0]] = i[1]
    for i in orderDict:
        value = orderDict[i]
        if isinstance(value, float):
            value = int(value)
        dataLst.append(value)
    return dataLst


def pickingS(tableName, columnName):
    # input : a column name
    # output : a list of all the data in a column
    # the output SHOULD not be ordered
    charSets = r.smembers('Sets')
    setsToFind = []
    orderDict = {}
    dataLst = []
    for i in charSets:
        if columnName in i:
            setsToFind.append(i)
    # print(setsToFind)
    for j in setsToFind:
        rawValue = j.split(":")[1]
        setName = columnName + ":" + rawValue
        lst = r.smembers(setName)
        for k in lst:
            id = k.split(":")[1]
            orderDict[id] = rawValue
    # print(orderDict)
    for i in orderDict:
        value = orderDict[i]
        dataLst.append(value)
    # print(dataLst)
    return dataLst


def update(query):
    # ******* Parsing Part *******
    # output : tableName, subquery for set, subquery for where
    query = query.replace(';', '')
    array = query.split()
    tableName = array[1]
    # print('TableName: '+tableName)
    pattern1 = 'set '
    pattern2 = 'where '
    s = re.compile(pattern1, re.IGNORECASE)
    w = re.compile(pattern2, re.IGNORECASE)
    matchSet = s.search(query)
    matchWhere = w.search(query)
    if matchWhere is not None:
        setQuery = query[matchSet.end():matchWhere.start()]
        whereQuery = query[matchWhere.end():]
        # print('SetQuery: '+setQuery)
        # print('WhereQuery: '+whereQuery)
    else:
        setQuery = query[matchSet.end():]
        # print('SetQuery'+setQuery)

    # ******* Redis Part *******
    if matchWhere is None:
        settingLst = setQuery.split(",")
        # print(settingLst)
        for a in settingLst:
            # for each column (Changing column)
            attName = a.split("=")[0]
            attName = attName.replace(" ", "")
            changedValue = a.split("=")[1]

            # META hash updating
            sz = r.hget("META:" + tableName, "size")
            tp = r.hget("META:" + tableName, attName)
            if ('int' in tp) or ('INT' in tp):
                changedValue = int(changedValue)

            # Row hash updating
            num = 0
            originalVals = []
            while (num < int(sz)):
                hName = tableName + ":" + str(num + 1)
                originalVals.append(r.hget(hName, attName))
                r.hset(hName, attName, changedValue)
                num = num + 1
                # print(r.hgetall(hName))
            nsName = attName + ":" + str(changedValue)

            # Column:val Set updating
            for i in originalVals:
                sName = attName + ":" + i
                num = 0
                while (num < int(sz)):
                    mName = tableName + ":" + str(num + 1)
                    # print(r.smembers(sName))
                    # print(r.sismember(sName,mName))
                    if (r.sismember(sName, mName)):
                        r.smove(sName, nsName, mName)
                    num = num + 1
            # set이 잘 옮겨졌나 확인
            name = r.smembers(nsName)
            # print(name)

            # Type이 int라면 zset에서 score 바꿔주기
            if ('int' in tp) or ('INT' in tp):
                zName = tableName + ":" + attName
                # print(zName)
                itemNum = r.zcard(zName)
                # print(itemNum)
                i = 1
                while (i <= itemNum):
                    r.zadd(zName, changedValue, i)
                    i = i + 1

            # Type이 varchar라면 Sets에 넣어주거나 아무것도 안 들어있는 세트 이름과 존재 지워주기
            else:
                for i in originalVals:
                    originalSet = attName + ":" + i
                    if (r.sismember("Sets", originalSet)):
                        if (r.scard(originalSet) == 0):
                            r.srem("Sets", originalSet)
                    if (r.sismember("Sets", nsName) == False):
                        r.sadd("Sets", nsName)
    else:
        # if there's WHERE..
        condition = whereQuery.split(" and")
        print(condition)


def delete(query):
    # ******* Parsing Part *******
    # output : tableName, subquery for set, subquery for where
    query = query.replace(';', '')
    array = query.split()
    pattern1 = 'from '
    pattern2 = 'where '
    f = re.compile(pattern1, re.IGNORECASE)
    w = re.compile(pattern2, re.IGNORECASE)
    matchFrom = f.search(query)
    matchWhere = w.search(query)
    if matchWhere is None:
        # if WHERE does not exist
        tableName = query[matchFrom.end():]
        # print(tableName)
    else:
        # if WHERE exists
        tableName = query[matchFrom.end():matchWhere.start()]
        whereQuery = query[matchWhere.end():]
        # print(tableName)
        # print(whereQuery)

    # ******* Redis Part *******
    if matchWhere is None:
        # DELETE FROM T1;

        # META 해시에서 column 정보 받아오기
        metaHash = "META:" + tableName
        fields = r.hkeys(metaHash)
        columnList = []
        sz = r.hget(metaHash, 'size')
        for i in fields:
            if 'size' not in i:
                columnList.append(i)
        # print(columnList)

        # 각각의 column들
        for column in columnList:
            if ('int' in r.hget(metaHash, column)) or ('INT' in r.hget(metaHash, column)):
                # print("We're gonna remove INTEGER! "+column)
                # if the column contains Integer
                id = 1
                zName = tableName + ":" + column
                score = []

                # tableName, column으로 zSet에서 값(score)들 찾아내 저장하고 zSet의 값들 지우기
                while (id <= int(sz)):
                    s = r.zscore(zName, id)
                    s = int(s)
                    score.append(s)
                    # print("delete"+zName+"!")
                    r.zrem(zName, id, s)
                    left = r.zcard(tableName + ":" + column)
                    # print("indices left:"+str(left))
                    id = id + 1
                r.delete(zName)
                # print(score)

                # zSet에서 뽑은 score들로 Set 접근해서 tableName 겹치는 거 지우기
                for j in score:
                    j = int(j)
                    sName = column + ":" + str(j)
                    id2 = 1
                    while (id2 <= int(sz)):
                        eName = tableName + ":" + str(id2)
                        # print(sName)
                        if r.sismember(sName, eName):
                            # print("delete "+eName+" in "+sName+"!")
                            r.srem(sName, eName)
                        id2 = id2 + 1
                    if r.scard(sName) == 0:
                        r.delete(sName)
            else:

                # print("We're gonna remove CHAR!")
                # if the column contains varchar
                # column, tableName

                # Sets에서 해당 column이 들어간 이름 모두 뽑기
                allSets = r.smembers('Sets')
                filteredSets = []
                for s in allSets:
                    if column in s:
                        filteredSets.append(s)

                # 해당 tableName:column 셋에서 row 번호들 지워주고, 빈 셋이면 Sets에서 이름 없애주기
                for f in filteredSets:
                    members = r.smembers(f)
                    for m in members:
                        if tableName in m:
                            r.srem(f, m)
                    if (r.scard(f) == 0):
                        r.srem('Sets', f)

        # 각 row와 Hash row 지우기
        i = 1
        while (i <= int(sz)):
            r.delete(tableName + ":" + str(i))
            i = i + 1
        r.hset(metaHash, 'size', 0)
        # 안에 요소들과 값들은 지워주되 table의 메타정보와 테이블 그 자체는 남겨야. size var는 0이 된다.


def show():
    # done
    tablelist = r.smembers('Tables')
    print("==============")
    print("Tables")
    print ("==============")
    for t in tablelist:
        newt = t.split(":")
        print newt[1]
    print("==============")


def flush():
    r.flushall()
    print("All data flushed!")


if __name__ == '__main__':
    hello_redis()