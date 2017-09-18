import csv
import pyodbc
from azure.storage.file import FileService
from io import StringIO

def fileService():
    file_service = FileService(account_name="storageprojetoestagio",account_key="7UsnLUlgJEExfzgjk/V7ijVXwzrF20D72lRaoT45MqUsRjDJwDtZl1Y5TAXtFQyl2F7bODljRRNCMJpCj6qGiw==")
    return file_service

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



def readFile(fileService):
    file_service = fileService
    azurestorage_text = file_service.get_file_to_text('metricas', 'teste', 'retorno.csv').content
    return azurestorage_text

#Estou verificando se há uma maneira de pegar o azure o share_name e file_name para não deixar essas info fixas no código
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
    dados = parseFile(readFile(fileService()))
    print(dados)

    #Existe o método executemany(),mas por trás ela faz vários inserts também, e em testes de desempenho ela se mostrou mais lenta. Estão atualizando o método mas ainda não está  100% pronto
    for line in range(dados.__len__()):
        cursor.execute("INSERT INTO metricas(data_, servidor, metrica, max_mb, avg_mb, p90_mb) VALUES (?,?,?,?,?,?)", (dados[line]))
    conn.commit()
    conn.close()


def delAzFile(fileService):
    file_service = fileService
    x = file_service.delete_file('metricas', 'teste', 'retorno.csv')
    print(x)

def do():
    dbInsertion(dbConnection())
    delAzFile(fileService())