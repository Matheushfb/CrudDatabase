import re
import pyodbc
import boto3


class Database:
    def __init__(self,port: int,conn: str,db_name: str,user_name: str,user_password: str,bucket: str,prefix: str):
        self.conn = conn
        self.db_name = db_name
        self.user_name = user_name
        self.user_password = user_password
        self.bucket = bucket
        self.prefix = prefix
        self.port = port

    def createdatabase(self, conn, db_name: str) -> str:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
        resultado = cursor.fetchone()
        if resultado == None:
            cursor.execute(f"CREATE DATABASE {db_name} ")
            print (f"banco de dados {db_name} criado com sucesso!")
        else:
            print (f"banco de dados {db_name} já existe!")
        conn.close()

    def createuser(self, conn, user_name:str, user_password:str) -> str:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sys.server_principals WHERE name = '{user_name}'")
        resultado = cursor.fetchone()
        if resultado == None:
            cursor.execute(f"CREATE LOGIN {user_name} WITH PASSWORD = N'{user_password}', DEFAULT_DATABASE = master, CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;")
        cursor.execute(f" USE {db_name}")
        cursor.execute(f"SELECT name FROM sys.server_principals WHERE name = '{user_name}'")
        resultado = cursor.fetchone()
        if resultado == None:
            cursor.execute(f"CREATE USER {user_name} FOR LOGIN {user_name};")
            print (f"usuário {user_name} criado")
        else:
            print (f"usuário {user_name} já existe")
        conn.close()

    def getalldatabases(self, conn) -> list:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sys.databases")
        databases = cursor.fetchall()
        print(type(databases))
        conn.close()
        return databases

    def getdatabase(self, conn, db_name) -> str:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
        database = cursor.fetchone()
        conn.close()
        return database

    def update(self,conn,db_name,db_table,data_update,collumn_update):
        cursor = conn.cursor()
        cursor.execute(f" USE {db_name}")
        cursor.execute(f"UPDATE {db_table} SET {collumn_update}='{data_update}' WHERE id=0")
        updated = cursor.commit()
        rowcount = cursor.rowcount
        #cursor.commit()
        conn.close()
        return rowcount

    '''
    Para execução desse código foram necessárias algumas alterações como por exemplo a remoção de blocos comentados feita na linha 78,
    devido o split que o python faz na linha 80 os primeiros caracteres de comentarios dão problema por não serem reconhecidos pelo pyodbc.
    Algumas linhas estavam com o batch delimiter GO escrito minusculo(go) alterei na linha de comandos os 8 casos em que estavam
    assim.
    '''

    def configuredatabase(self, conn,db_name: str):
        cursor = conn.cursor()
        cursor.execute(f" USE {db_name}")
        c = boto3.client('s3')
        response = c.list_objects_v2(Bucket=bucket,Prefix=prefix)
        for obj in response.get('Contents',[]):
            obj = c.get_object(Bucket=bucket,Key=obj['Key'])
            response_body = obj['Body']
            sql_script = response_body.read().decode('utf-8')
            script_sem_comentarios = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
            if script_sem_comentarios != "":
                for statement in re.split('GO' , script_sem_comentarios):
                    print(f"Executando query {statement}")
                    cursor.execute(statement)
        conn.close()

