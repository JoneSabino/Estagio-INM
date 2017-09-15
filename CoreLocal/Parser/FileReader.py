import csv
import pyodbc
from azure.storage.file import FileService
from io  import StringIO


file_service = FileService(account_name="jonesabinostorage",account_key="QvgR5kwDrFN4OYkWp+s3S9QAaSDhky9RuUPMMw0QgfdZEnx7LG9WfiByFhHO+aNYaWKiMp31G86Ltz5fvDNJKA==")

conn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};'
                      'Server=tcp:dbteste-server.database.windows.net,1433;'
                      'Database=MetricasTeste;Uid=dbteste@dbteste-server;'
                      'Pwd=inmetrics123#;'
                      'Encrypt=yes;'
                      'TrustServerCertificate=no;'
                      'Connection Timeout=30;')

dados=[]

#Estou verificando se há uma maneira de pegar o azure o share_name e file_name para não deixar essas info fixas no código
azurestorage_text = file_service.get_file_to_text('teste', '', 'Retorno.csv').content

with StringIO(azurestorage_text) as file_obj:
    reader = csv.reader(file_obj, delimiter=';')
    header = next(reader)
    for line in reader:
         dados.append(line)

cursor = conn.cursor()

#Existe o método executemany(),mas por trás ela faz vários inserts também, e em testes de desempenho ela se mostrou mais lenta. Estão atualizando o método mas ainda não está  100% pronto
for line in range(dados.__len__()):
    cursor.execute('INSERT INTO CPU_metrics (data_,servidor,metrica,max_mb,avg_mb,p90_mb) VALUES (?,?,?,?,?,?)', (dados[line]))

conn.commit()
conn.close()