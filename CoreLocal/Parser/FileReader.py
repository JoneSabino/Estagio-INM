import csv
import pyodbc
from azure.storage.file import FileService
from io import StringIO

file_service = FileService(account_name="storageprojetoestagio",account_key="7UsnLUlgJEExfzgjk/V7ijVXwzrF20D72lRaoT45MqUsRjDJwDtZl1Y5TAXtFQyl2F7bODljRRNCMJpCj6qGiw==")
def getNames():
    share_name = ''
    for i in file_service.list_shares():
        share_name = i.name
    return share_name
share_name = getNames()

def verifyShare():
    if file_service.exists(share_name, 'teste', 'retorno.csv'):
        return True
    else:
        return False


def dbConnection():
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
    azurestorage_text = file_service.get_file_to_text(share_name, 'teste', 'retorno.csv').content
    return azurestorage_text

def parseFile(readFile):
    dados = []
    azurestorage_text = readFile

    with StringIO(azurestorage_text) as file_obj:
        reader = csv.reader(file_obj, delimiter=';')
        header = next(reader)
        for line in reader:
            if line != []:
                dados.append(line)
    return dados

def dbInsertion(dbConnection):
    conn = dbConnection
    cursor = conn.cursor()
    dados = parseFile(readFile())
    # print(dados)

    #Existe o método executemany(),mas por trás ela faz vários inserts também, e em testes de desempenho ela se mostrou mais lenta. Estão atualizando o método mas ainda não está  100% pronto
    for line in range(dados.__len__()):
        cursor.execute("INSERT INTO metricas(data_, servidor, metrica, max_mb, avg_mb, p90_mb) VALUES (?,?,?,?,?,?)", (dados[line]))
    conn.commit()
    conn.close()


def delAzFile():
    file_service.delete_file(share_name, 'teste', 'retorno.csv')

def do():
    dbInsertion(dbConnection())
    delAzFile()