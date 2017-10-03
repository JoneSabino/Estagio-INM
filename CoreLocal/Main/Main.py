import csv
import pyodbc
from azure.storage.file import FileService
from io import StringIO
import time
import os
import json

file_service = FileService(account_name="storageprojetoestagio",account_key="7UsnLUlgJEExfzgjk/V7ijVXwzrF20D72lRaoT45MqUsRjDJwDtZl1Y5TAXtFQyl2F7bODljRRNCMJpCj6qGiw==")

########################################################################################################################
def CreateCursor(CONNECTION_STRING):
    return pyodbc.connect(CONNECTION_STRING).cursor()

def encapsulate(value):
    # Encapsulate the input for SQL use (add ' etc)
    if isinstance(value, list):
        r = []
        for i in value:
            if isinstance(i, str):
                r.append(encapsulate(i))
            else:
                r.append(str(encapsulate(i)))
        return ','.join(r)
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
        return "'" + value.isoformat() + "'"
    elif value is None:
        return "Null"
    else:
        return str(value)
        # cursor,user.fbid,user.name,user.age,user.gender,user.device,user.platform,DeviceTypesDic, platformTypesDic

def enlist(rows):
    # Takes a list of items and make them in format for SQL insert
    # limit of 1000 lines
    clLists = []
    cl = []
    LineCounter = 0
    for i in rows:
        if LineCounter >= 1000:
            clLists.append(",".join(cl))
            cl = []
            LineCounter = 0
        cl.append("(" + encapsulate(i) + ")")
        LineCounter += 1
    clLists.append(",".join(cl))
    return clLists

def CreateDicForTable(table, cursor):
    cursor.execute("select * from %s" % table)
    rows = cursor.fetchall()
    dic = {}
    for row in rows:
        dic[row[1]] = row[0]
    return dic

def RefeshDicForTable(table, TableDic, NewItemsList, cursor):
    # Gets table dictionary and new items rows, add the new items to the DB and update the Dict with new keys
    cursor.execute("select isnull(max(%sID),0) as MaxID from %s" % (table[:-1], table))
    maxID = cursor.fetchone()[0]
    for NewItems in enlist(NewItemsList):
        cursor.execute("Insert into %s (Name) Values " % table + NewItems)
    cursor.commit()
    cursor.execute("select * from %s where %sID > %d" % (table, table[:-1], maxID))
    rows = cursor.fetchall()
    dic = TableDic
    for row in rows:
        dic[row[1]] = row[0]
    return dic

def Dic2List(ListofDicts):
    if len(ListofDicts) > 0:
        fields = ListofDicts[0].keys()
        ll = []
        for dic in ListofDicts:
            dictlist = []
            for key, value in dic.iteritems():
                dictlist.append(value)
            ll.append(dictlist)
    else:
        fields = []
        ll = []
    return (fields, ll)

def enfields(fields):
    tempfields = []
    for field in fields:
        tempfields.append('[%s]' % field)
    return "(" + ",".join(tempfields) + ")"

def insertListDic(ListofDicts, table_name, fieldsOverRide, cursor):
    # must get only one table
    (fields, datas) = Dic2List(ListofDicts)
    if fieldsOverRide != "" and fieldsOverRide is not None:
        fields = fieldsOverRide
    for data in enlist(datas):
        #    print ("Insert into %s %s Values %s" % (table_name, enfields(fields), data))
        cursor.execute("Insert into %s %s Values %s" % (table_name, enfields(fields), data))
    cursor.commit()

def CreateDicForTypesTable(table, cursor):
    cursor.execute("select * from %s" % table)
    rows = cursor.fetchall()
    dic = {}
    for row in rows:
        dic[row[1]] = row[0]
    return dic

def updateTypeDic(TypeDic, table_name, CodeName, dataset, cursor):
    if len(dataset) > 0:
        dataset -= set(TypeDic)
        if len(dataset) > 0:
            cursor.execute("select isnull(max(%s),0) as MaxID from %s" % (CodeName, table_name))
            maxID = cursor.fetchone()[0]
            for data in enlist(dataset):
                cursor.execute("Insert into %s (Name) Values %s" % (table_name, data))
                cursor.commit()
            cursor.execute("select * from %s where %s > %d" % (table_name, CodeName, maxID))
            rows = cursor.fetchall()
            for row in rows:
                TypeDic[row[1]] = row[0]
    return TypeDic

def writetoDB(datalist, table_name, fieldsnames, cursor):
    for data in enlist(datalist):
        cursor.execute("Insert into %s %s Values %s" % (table_name, enfields(fieldsnames), data))
    cursor.commit()
########################################################################################################################


def getNames():
    share_name = ''
    for i in file_service.list_shares():
        share_name = i.name
    return share_name

# share_obj = StringIO(getNames())
# share_name = next(share_obj)
share_name = getNames()
# print(share_name)


def verifyShare():
    print('\nGetting informations from Azure Storage')
    print('File Service Share Name: ' + share_name)
    if file_service.exists(share_name, '', 'retorno.csv'):
        print('\nFile found. Preparing to read')
        return True
    else:
        print('\nFile not found, finishing module')
        return False

connstr = 'Driver={ODBC Driver 13 for SQL Server};Server=tcp:srv-projeto-estagio.database.windows.net,1433;Database=db-projeto-estagio;Uid=projeto-estagio@srv-projeto-estagio;Pwd=inmetrics@123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
def dbConnection():
    print('\nConnecting to the database...')
    conn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};'
                          'Server=tcp:srv-projeto-estagio.database.windows.net,1433;'
                          'Database=db-projeto-estagio;'
                          'Uid=projeto-estagio@srv-projeto-estagio;'
                          'Pwd=inmetrics@123;'
                          'Encrypt=yes;'
                          'TrustServerCertificate=no;'
                          'Connection Timeout=30;')
    return conn


def readFile():
    print('\nReading file...')
    azurestorage_text = file_service.get_file_to_text(share_name, '', 'retorno.csv').content
    return azurestorage_text


def parseFile(readFile):
    dados = []
    azurestorage_text = readFile
    print('\nParsing file...')

    with StringIO(azurestorage_text) as file_obj:
        reader = csv.reader(file_obj, delimiter=';')
        header = next(reader)
        # print(header)
        for line in reader:
            if line != []:
                dados.append(line)
    return dados


def dbInsertion():
    # cursor = conn.cursor()
    cursor = CreateCursor(connstr)
    dados = parseFile(readFile())
    print('\nSending data to Azure SQL...')
    start = time.clock()
    # for line in range(dados.__len__()):
    for line in enlist(dados):
        #print(line)
        cursor.execute("INSERT INTO metricas(data_, servidor, metrica, max_mb, avg_mb, p90_mb) VALUES " + line)
    cursor.commit()
    end = time.clock()
    cursor.close()
    sec = end - start
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    print("%d:%02d:%02d" % (h, m, s))


def delAzFile():
    print('\nDeleting file from Azure Storage')
    file_service.delete_file(share_name, '', 'retorno.csv')

def main():
    dbInsertion()
    delAzFile()


if __name__ == '__main__':
    print('Starting process...\n')
    print('Connecting to Azure Storage')
    time.sleep(1)
    if verifyShare():
        main()
    else:
        print('A new verification will start in 5 minutes')
