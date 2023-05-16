from connection import Connect
import json
import pprint

db = Connect()

sql = "SELECT emp.codigo, emp.razaosocial FROM emp"

resultados = db.execute_query(sql)
data = []

for resultado in resultados:
    values = {
        'cod_fortes': resultado[0],
        'nome_fantasia': resultado[1]
    }
    data.append(values)

with open('cod_empresas.json', 'w') as f:
    json.dump(data, f)

pprint.pprint(data)
