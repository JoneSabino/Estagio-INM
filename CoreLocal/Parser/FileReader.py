import csv
import pyodbc
from azure.storage.file import FileService
from io import StringIO
import time
import os
import json

file_service = FileService(account_name="storageprojetoestagio", account_key="7UsnLUlgJEExfzgjk/V7ijVXwzrF20D72lRaoT45MqUsRjDJwDtZl1Y5TAXtFQyl2F7bODljRRNCMJpCj6qGiw==")

def getnames():
    share_name = ''
    file_name = ''
    for i in file_service.list_shares():
        share_name = i.name
    for i in file_service.list_directories_and_files(share_name):
        file_name = i.name
    return share_name, file_name

share_name = getnames()[0]
file_name = getnames()[1]


def verifyshare():
    print('\nGetting informations\n')
    print('File Service - Share Name: ' + share_name)
    if file_name == '':
        print('\nThere are no files to download, finishing module')
        return False
    else:
    #elif file_service.exists(share_name, '', file_name):
        print('\nFile found: ' + file_name + '\nPreparing to read...')
        return True



def dbconnection():
    conn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};'
                          'Server=tcp:srv-projeto-estagio.database.windows.net,1433;'
                          'Database=db-projeto-estagio;'
                          'Uid=projeto-estagio@srv-projeto-estagio;'
                          'Pwd=inmetrics@123;'
                          'Encrypt=yes;'
                          'TrustServerCertificate=no;'
                          'Connection Timeout=30;')
    return conn


def readfile():
    print('\nReading file content...')
    azurestorage_text = file_service.get_file_to_text(share_name, '', file_name).content
    return azurestorage_text


def parsefile(readfile):
    dados = []
    azurestorage_text = readfile
    print('\nPreparing data to be stored...')

    with StringIO(azurestorage_text) as file_obj:
        reader = csv.reader(file_obj, delimiter=';')
        header = next(reader)
        for line in reader:
            if line != []:
                dados.append(line)
    return dados


def dbinsertion(dbconnection):
    dados = parsefile(readfile())
    print('\nConnecting to Azure SQL...')
    conn = dbconnection
    cursor = conn.cursor()
    print('\nWriting data...')
    for line in range(dados.__len__()):
        cursor.execute("INSERT INTO metricas(data_, servidor, metrica, max_mb, avg_mb, p90_mb) VALUES (?,?,?,?,?,?)",
                       (dados[line]))
    conn.commit()
    conn.close()


def delazfile():
    print('\nDeleting file from Azure Storage')
    file_service.delete_file(share_name, '', file_name)
    print('\n File "'+ file_name +'" has been deleted successfully')
    print('\nA new verification will start in 5 minutes\n')


def main():
    dbinsertion(dbconnection())
    delazfile()


if __name__ == '__main__':
    print('\nAccessing Azure Storage')
    time.sleep(2)
    if verifyshare():
        main()
    else:
        print('A new verification will start in 5 minutes\n')

