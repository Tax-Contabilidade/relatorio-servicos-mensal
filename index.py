import time
import json
import locale
import calendar
from enum import Enum
from datetime import datetime
from connection import Connect, MySqlCon


class servicos(Enum):
    NFE = "N. Fisc. Eletrônica"
    NFSE = "N. Fisc Serviço Eletrônica"
    NVC = "Nota Fiscal de Venda a consumidor"
    CFC = "Cupons Fiscais"
    CPE = "Cupons Fiscais Eletrônicos"

# entrada = nfe, nfse
# saida = nvc, 
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
DB = Connect()
MCON = MySqlCon(db="services")

with open('cod_empresas.json', 'r') as file:
    data = json.load(file)

class Main():

    def __init__(self, cod_fortes, data_inicial, razao_social):
        self.cod_fortes = cod_fortes
        self.data_inicial = datetime.strptime(data_inicial, "%d/%m/%Y")
        self.data_final = self.data_inicial.replace(day=(calendar.monthrange(self.data_inicial.year, self.data_inicial.month)[1]))
        self.data_formatada = self.data_inicial.strftime("%Y-%m-%d")
        self.razao_social = razao_social

    class Report():

        def __init__(self, cod_fortes, razao_social, referencia, entradas, saidas, funcionarios, chamados):
            self.db = MCON
            self.cod_fortes = cod_fortes
            self.razao_social = razao_social
            self.referencia = referencia
            self.entradas = entradas
            self.saidas = saidas
            self.funcionarios = funcionarios
            self.chamados = chamados

        def send_report(self):
            db = self.db
            sql = f"""
                INSERT INTO services 
                VALUES({self.cod_fortes}, {self.razao_social}, {self.referencia}, 
                    {self.entradas}, {self.saidas}, {self.funcionarios}, {self.chamados})
            """
            print(sql)
            time.sleep(50)

            db.execute_query(sql)

    class Servicos():
        
        def __init__(self, cod_fortes:str, data_inicial:str):
            self.db = DB
            self.cod_fortes = cod_fortes
            self.__data_ini = datetime.strptime(data_inicial, "%d/%m/%Y")
            self.data_inicial = self.__data_ini.strftime("%m/%d/%Y")
            self.data_final = self.__data_ini.replace(day=(calendar.monthrange(self.__data_ini.year, self.__data_ini.month)[1])).strftime("%m/%d/%Y")
            self.entradas = 0
            self.saidas = 0
            self.counter = {
                            'saidas': {'NFEM':0, 'NFS':0, 'NVC':0, 'CFE':0}, 
                            'entradas': {'NFEM':0, 'NFS':0}, 
                            }
            
        def calculate_total(self):
            saidas = sum([v for v in self.counter['saidas'].values()])
            entradas = sum([v for v in self.counter['entradas'].values()])
            self.counter['total'] = saidas + entradas 
            self.entradas = entradas
            self.saidas = saidas

        def count_nfe(self, OP:str):
            db = self.db
            sql = f"""
                SELECT nfm.seq, nfm.emp_codigo
                    FROM nfm 
                    WHERE nfm.emp_codigo = '{self.cod_fortes}' AND nfm.operacao='{OP}'
                    AND NFM.dtentradasaida BETWEEN '{self.data_inicial}' AND '{self.data_final}'
                """
            resultado = db.execute_query(sql)

            chave = 'saidas' if OP == "S" else 'entradas'
            self.counter[chave]['NFEM'] += len(resultado)
            return resultado
        
        def count_nf_venda_consumidor(self):
            db = self.db
            sql = f"""
                SELECT nvc.seq, nvc.emp_codigo
                    FROM nvc 
                    WHERE nvc.emp_codigo = '{self.cod_fortes}'
                    AND nvc.dtdoc between '{self.data_inicial}' AND '{self.data_final}'
                """
            resultado = db.execute_query(sql)

            self.counter['saidas']['NVC'] += len(resultado)
            return resultado
        
        def count_nf_servico(self, OP:str):
            db = self.db
            sql = ""
            if OP == "S":
                sql = f"""
                    SELECT dss.seq, dss.emp_codigo
                        FROM dss 
                        WHERE dss.emp_codigo='{self.cod_fortes}' AND dss.cancelado='0' 
                        AND dss.dtprestacao BETWEEN '{self.data_inicial}' AND '{self.data_final}'
                    """
            elif OP == "E":
                sql = f"""
                    SELECT esi.seq, esi.emp_codigo
                        FROM esi 
                        WHERE esi.emp_codigo='{self.cod_fortes}'
                        AND esi.dtemissao BETWEEN '{self.data_inicial}' AND '{self.data_final}'
                    """
                
            resultado = db.execute_query(sql)

            chave = 'saidas' if OP == "S" else 'entradas'
            self.counter[chave]['NFS'] += len(resultado)
            return resultado
            
        def count_cupom_fiscal(self):
            db = self.db
            sql = f"""
                SELECT cpe.seq, cpe.emp_codigo
                    FROM cpe 
                    WHERE cpe.emp_codigo = '{self.cod_fortes}'
                    AND cpe.data between '{self.data_inicial}' AND '{self.data_final}'
                """
            resultado = db.execute_query(sql)

            self.counter['saidas']['CFE'] += len(resultado)
            return resultado
        
        def calcular_saidas(self):
            notas_fiscais = self.count_nfe("S")
            notas_servico = self.count_nf_servico("S")
            notas_vendas = self.count_nf_venda_consumidor()

        def calcular_entradas(self):
            notas_fiscais = self.count_nfe("E")
            notas_servico = self.count_nf_servico("E")

        def show(self):
            self.calculate_total()
            output = f"""\n
            Relatório {self.cod_fortes} Período: {self.data_inicial} a {self.data_final}\n
            Saídas: {self.counter['saidas']}\n
            Entradas: {self.counter['entradas']}\n
            Total {self.__data_ini.strftime("%B")}: {self.counter['total']}\n
            """
            print(output)

    class Funcionarios():

        def __init__(self, cod_fortes:str, data_inicial:str):
            self.db = DB
            self.cod_fortes = cod_fortes
            self.__data_ini = datetime.strptime(data_inicial, "%d/%m/%Y")
            self.data_inicial = self.__data_ini.strftime("%m/%d/%Y")
            self.data_final = self.__data_ini.replace(day=(calendar.monthrange(self.__data_ini.year, self.__data_ini.month)[1])).strftime("%m/%d/%Y")
            self.funcionarios = 0

        def count_funcionarios(self):
            db = self.db
            sql = f"""
                SELECT * FROM epg WHERE epg.emp_codigo='{self.cod_fortes}'
            """
            self.funcionarios = len(db.execute_query(sql))
            return self

        def show(self):
            print(f"Quantidade de funcionários: {self.funcionarios}")

    def perform(self):
        services = self.Servicos(cod_fortes=self.cod_fortes, data_inicial=f"01/{self.data_inicial.month}/2023")
        services.calcular_saidas()
        services.calcular_entradas()
        services.show()

        employees = self.Funcionarios(cod_fortes=self.cod_fortes, data_inicial=f"01/{self.data_inicial.month}/2023")
        employees.count_funcionarios().show()

        report = self.Report(self.cod_fortes, self.razao_social, self.data_formatada, services.entradas, 
                             services.saidas, employees.funcionarios, "TO DO")
        report.send_report()

if __name__ == "__main__":
    for item in data:
        cod = item['cod_fortes']
        razao_social = item['nome_fantasia']
        for month in range(1, 13):
            main = Main(cod_fortes=cod, data_inicial=f"01/{month}/2023", razao_social=razao_social)
            main.perform()