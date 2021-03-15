import pyodbc
import json
import requests

''' Conexão com o DB. 
    Defini se o banco de dados é local ou na núvem.
    Retorna um cursor de conexão aberta
    Por defaul a conexão será local
'''
def conexaoDB (tipo = 'local'):
    if tipo == 'azure':
        conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=casadocodigo-sql-srv-isr.database.windows.net;'
                      'Database=BD_SQL_GAMMA_IBGE;'
                      'UID=Administrador;'
                      'PWD=Alura!123;')
    else:
        conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-DPP33GN;'
                      'Database=BD_SQL_GAMMA_IBGE;'
                      'UID=sa;'
                      'PWD=sa;')

    return conn.cursor()

''' 
    Realiza uma chamada ao serviço do IBGE e retona um json    
'''
def requisicao_api():
    resposta = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')
    if resposta.status_code == 200:
        return resposta.json()
    else:
        return resposta.status_code

'''
    Consulta se um determinado registro já está cadastrado na tabela.
    Retorna um True para existente e false para não
'''
def consulta_tabela (tabela, id):
    cursor = conexaoDB()
    cons = cursor.execute(f"SELECT id FROM {tabela} WHERE ID = ?", id)
    s = [True for x in cons if x[0] == id]
    resp = False
    if len(s) == 1:
        resp = True
    return resp

'''
    Verifica se o registro existe e seão existir, carrega na tabela
'''
def consulta_carrega(tabela, id, nome, sigla = '', fk=0, fk2=0):
    cursor = conexaoDB()
    cons = consulta_tabela(tabela, id)
    if cons != True:
        if tabela == 'REGIAO':
            cursor.execute(f"INSERT INTO {tabela} VALUES (?,?,?)", id, sigla, nome)
        elif tabela == 'UF':
            cursor.execute(f"INSERT INTO {tabela} VALUES (?,?,?,?)", id, sigla, nome, fk)
        elif tabela == 'CIDADE':
            cursor.execute(f"INSERT INTO {tabela} VALUES (?,?,?,?)", id, nome, fk, fk2)
        else:
            cursor.execute(f"INSERT INTO {tabela} VALUES (?,?,?)", id, nome, fk)
        cursor.commit()

'''
    Realiza uma consulta ao serviço e recebe um json para ser iterado
    Durante a iteração, verifica se o distrito já está cadastrado na tabela. Se não estive
    percorre toda a hierarquia, verificando e cadastrando as dependências até cadatrar o 
    distrito.
'''
def carrega_distrito():
    resposta = requisicao_api()

    for js in resposta:

        id_dist = (js['id'])
        ex = consulta_tabela('DISTRITO', id_dist)

        if ex != True:
            # REGIAO
            id = (js['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id'])
            sigla = (js['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla'])
            nome = (js['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome'])
            consulta_carrega('REGIAO', id, nome, sigla)

            # UF
            id = (js['municipio']['microrregiao']['mesorregiao']['UF']['id'])
            sigla = (js['municipio']['microrregiao']['mesorregiao']['UF']['sigla'])
            nome = (js['municipio']['microrregiao']['mesorregiao']['UF']['nome'])
            fk = (js['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id'])
            consulta_carrega('UF', id, nome, sigla, fk)

            # Mesorregiao
            id = (js['municipio']['microrregiao']['mesorregiao']['id'])
            nome = (js['municipio']['microrregiao']['mesorregiao']['nome'])
            fk = (js['municipio']['microrregiao']['mesorregiao']['UF']['id'])
            sigla = ''
            consulta_carrega('MESORREGIAO', id, nome, sigla, fk)

            # Região intermediária - Está no mesmo nível que mesorregião
            id = (js['municipio']['regiao-imediata']['regiao-intermediaria']['id'])
            nome = (js['municipio']['regiao-imediata']['regiao-intermediaria']['nome'])
            fk = (js['municipio']['regiao-imediata']['regiao-intermediaria']['UF']['id'])
            sigla = ''
            consulta_carrega('REGIAO_INTERMEDIARIA', id, nome, sigla, fk)

            # Microrregiao
            id = (js['municipio']['microrregiao']['id'])
            nome = (js['municipio']['microrregiao']['nome'])
            fk = (js['municipio']['microrregiao']['mesorregiao']['id'])
            sigla = ''
            consulta_carrega('MICRORREGIAO', id, nome, sigla, fk)

            # Região imediata - Está no mesmo nível de microrregião
            id = (js['municipio']['regiao-imediata']['id'])
            nome = (js['municipio']['regiao-imediata']['nome'])
            fk = (js['municipio']['regiao-imediata']['regiao-intermediaria']['id'])
            sigla = ''
            consulta_carrega('REGIAO_IMEDIATA', id, nome, sigla, fk)

            # Cidade: cidades são a mesma coisa que municípios.
            id = (js['municipio']['id'])
            nome = (js['municipio']['nome'])
            fk = (js['municipio']['microrregiao']['id'])
            fk2 = (js['municipio']['regiao-imediata']['id'])
            sigla = ''
            consulta_carrega('CIDADE', id, nome, sigla, fk, fk2)

            # Distrito: uma subdivisão de cidade. um conjunto de distritos definemuma região metropolitana dentro de um municipio
            id = (js['id'])
            nome = (js['nome'])
            fk = (js['municipio']['id'])
            sigla = ''
            consulta_carrega('DISTRITO', id, nome, sigla, fk)

