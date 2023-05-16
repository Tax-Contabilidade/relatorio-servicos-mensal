import fdb
import mysql
import mysql.connector as mcon

class Connect():

    def __init__(self, db:str = 'AC'):
        self.__con = fdb.connect(
                host='192.168.1.100',
                database=f'C:\\Banco\\{db}.FDB',
                port=53052,
                user='sysdba',
                password='masterkey',
                charset='ISO8859_1'
            )
        self.__cur = self.__con.cursor()

    def execute_query(self, sql):
        cur = self.__cur

        cur.execute(sql)
        return cur.fetchall()

class MySqlCon():

    def __init__(self, db):
        self.__con = mcon.connect(
                host='192.168.1.61',
                database=f'{db}',
                port=3306,
                user='admin',
                password='admin',
            )
        self.__cur = self.__con.cursor()

    def execute_query(self, sql):
        cur = self.__cur

        try:
            cur.execute(sql);
        except mysql.connector.Error as e:
            if e.errno == 1049:
                print("Deu certo")

        return cur.fetchall()