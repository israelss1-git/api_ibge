'''
Atividade carga de DB com dados de API do IBGE

Aluno: Israel Silva de Souza

Atividade 09 - Entrega 04/03, até 23:59:
Desenvolvimento de Script Python para leitura de dados da API do IBGE (https://servicodados.ibge.gov.br/api/docs/localidades) com o uso do package requests
(https://www.nylas.com/blog/use-python-requests-module-rest-apis/) para escrever em BD relacional alocado no AZURE os seguintes dados:
UFs
Cidades, relacionadas com UFs
Distritos, relacionados com Cidades
Os arquivos a serem entregues são o script SQL para criação deste schema e o código fonte Python
'''

import pyodbc
import requests

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=casadocodigo-sql-srv-isr.database.windows.net;'
                      'Database=BD_SQL_GAMMA;'
                      'UID=Administrador;'
                      'PWD=Alura!123;')

cursor = conn.cursor()

def requisicao_api(url_api):
    resposta = requests.get(url_api)

    if resposta.status_code == 200:
        return resposta.json()
    else:
        return resposta.status_code


# TABELA DE REGIÕES
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/regioes')
for reg in resposta:
    cursor.execute("INSERT INTO REGIAO VALUES (?,?,?)", reg['id'], reg['sigla'], reg['nome'])
    cursor.commit()

# TABELA DE UF
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/estados')
for uf in resposta:
    cursor.execute("INSERT INTO UF VALUES (?,?,?,?)", uf['id'], uf['sigla'], uf['nome'], uf['regiao']['id'])
    cursor.commit()

# TABELA DE MESORREGIAO
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes')
for meso in resposta:
    cursor.execute("INSERT INTO MESORREGIAO VALUES (?,?,?)", meso['id'], meso['nome'], meso['UF']['id'])
    cursor.commit()

# TABELA DE MICRORREGIAO
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes')
for micro in resposta:
    cursor.execute("INSERT INTO MICRORREGIAO VALUES (?,?,?)", micro['id'], micro['nome'], micro['mesorregiao']['id'])
    cursor.commit()

# TABELA DE CIDADE
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/municipios')
for mun in resposta:
    cursor.execute("INSERT INTO CIDADE VALUES (?,?,?)", mun['id'], mun['nome'], mun['microrregiao']['id'])
    cursor.commit()

# TABELA DE DISTRITO
resposta = requisicao_api('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')

for dis in resposta:
    '''print(dis['id'])
    print(dis['nome'])
    print(dis['municipios']['id'])'''
    cursor.execute("INSERT INTO DISTRITO VALUES (?,?,?)", dis['id'], dis['nome'], dis['municipio']['id'])
    cursor.commit()