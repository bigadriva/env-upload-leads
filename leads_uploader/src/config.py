# -*- coding: utf-8 -*-
"""Este módulo é responsável pela configuração do backend e banco de dados quando os usuários sobem
as bases para o empurra leads."""

import os
import sys
import threading

import concurrent.futures

import numpy as np
import pandas as pd

from util import format_cnpj

from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
load_dotenv()


def send_leads(filename, company_name):
    # Pegando o nome do arquivo
    if len(sys.argv) == 3:
        filename = sys.argv[1]
        company_name = sys.argv[2]

    if filename and company_name:
        print(f'DATA: Recebido arquivo {filename} da empresa {company_name}.')
        sys.stdout.flush()

        mapping = create_mapping_from_csv(filename)

        # Pega apenas o nome do arquivo, sem a extensão, para o nome do index.
        index_name = filename.split('/')[1].split('.')[0]

        # Colocamos todas as letras minúsculas e...
        # Se tiver espaços no nome da empresa, substituímos por hífen (-)
        # Prefixa o nome do index com o nome da empresa que subiu a base.
        index_name = f"{company_name.replace(' ', '-')}-{index_name}".lower()


        if not index_exists_for_company(index_name):
            create_index(index_name, mapping)
        
            upload_all_leads(index_name, filename, company_name)

            print(f'END DATA: {index_name}')
            sys.stdout.flush()
        else:
            print('END DATA: O indice ja existe.')
            sys.stdout.flush()
    else:
        print('END DATA: Não foi passado nenhum arquivo.')
        sys.stdout.flush()


def index_exists_for_company(index_name: str) -> bool:
    """Checa se um índice existe.

    :param index_name:str: O nome do índice que queremos ver se existe
    """
    exists = False

    with Elasticsearch([os.environ['ELASTIC_URL']]) as elastic:
        if elastic.indices.exists(index_name):
            return True

    return exists

def create_index(index_name: str, mapping: dict) -> None:
    """Cria um index com o nome especificado e o mapping passado
    
    :param index_name:str: O nome do índice a ser criado.
    :param mapping:dict: O mapping do índice

    :returns: Não retorna nada.
    """
    with Elasticsearch([os.environ['ELASTIC_URL']]) as elastic:
        print(f'Criando índice {index_name}')
        sys.stdout.flush()
        elastic.indices.create(index=index_name)

        print('Inserindo mapping')
        sys.stdout.flush()
        elastic.indices.put_mapping(mapping, index=index_name)

def create_mapping_from_csv(csv_path: str) -> dict:
    """Cria um mapping a partir de um arquivo CSV.
    
    :param csv_path: O path do arquivo CSV a ser utilizado.
    
    :returns dict: O mapping gerado pelo respectivo CSV.
    """
    mapping = {
        'properties': {
            'date_sent': {
                'type': 'date'
            },
            
            'status': {
                'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 256
                    }
                }
            },
            
            'chat_id': {
                'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 256
                    }
                }
            }
        }
    }

    if not os.path.exists(csv_path):
        print('Arquivo não encontrado')
        sys.stdout.flush()
    try:
        df = pd.read_csv(csv_path).fillna('')
    except Exception as e:
        print('DATA: ', e)

    #TODO: Melhorar implementação
    # Obrigatoriamente, o arquivo deve conter uma coluna com o nome "cnpj", minúsculo.
    # Os CNPJs devem conter apenas números.
    # Aqui formatamos o CNPJ como string
    df['cnpj'] = format_cnpj(df['cnpj'])

    for column in df:
        add_field_type_to_mapping(mapping, df, column)

    return mapping

def add_field_type_to_mapping(mapping: dict, df: pd.DataFrame, column: str) -> None:
    """Adiciona o tipo do campo analisado ao mapping de acordo com a nomeação de campos do elastic
    
    :param mapping:dict: O mapping até o momento.
    :param df:pd.DataFrame: O dataframe do pandas guardando os dados do CSV da base utilizada.
    :param column:str: A coluna do dataframe sendo analisada.
    
    :returns None:
    """
    field_type = None

    column_type = df[column].dtype

    if column_type == 'int64':
        field_type = 'long'

    elif column_type == 'object':
        field_type = 'text'

    elif column_type == 'float64':
        field_type = 'float'

    # Se for a coluna date sent, retornamos de imediato. Fazemos isso para não sobrescrever o tipo
    # date, porque já aconteceu do elatic colocar como texto.
    if column == 'date_sent':
        return

    mapping['properties'][column] = {
        'type': field_type,
        'fields': {
            'keyword': {
                'type': 'keyword',
                'ignore_above': 256
            }
        }
    }

def upload_all_leads(index_name: str, csv_path: str, company_name: str) -> None:
    """Sobe todos os leads no CSV passado para o índice criado no elastic.
    
    :param index_name:str: O nome do índice criado para armazenar os dados.
    :param csv_path:str: O path do arquivo CSV utilizado para gerar o índice.
    
    :returns None:
    """
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path).fillna('')
        df['cnpj'] = format_cnpj(df['cnpj'])
        df = df.assign(company_name=company_name)

        leads = []
        for row_number in range(len(df)):
            row = df.loc[row_number, :]

            doc = create_doc(index_name, row)
            leads.append(doc)
        
        upload_leads(leads)
    else:
        print('DATA: Arquivo nao encontrado')
        sys.stdout.flush()

def create_doc(index_name: str, row: pd.Series):
    """Criar um documento no elastic com o lead passado.
    
    :param index_name:str: O nome do index a ser criado.
    :param row:pd.Series: O lead usado para criar o documento.
    """
    doc = {
        '_index': index_name,
        '_source': {
            'status': 'PENDING',
        }
    }
    for field, value in row.items():
        if value != np.nan:
            doc['_source'][field] = value

    return doc

def upload_leads(leads: list):
    """Realiza o upload dos leads passados."""
    with Elasticsearch([os.environ['ELASTIC_URL']]) as elastic:
        failed_uploads = []
        
        print('DATA: Aqui')
        filtered_leads = filter_companies(elastic, leads)
        print('DATA: Aqui')
        try:
            for success, info in helpers.parallel_bulk(elastic, filtered_leads):
                if not success:
                    failed_uploads.append(info)

        except Exception as e:
            print('DATA: [ _upload_leads ] Aconteceu algo de errado ao subir a base')
            print('DATA: [ _upload_leads ]', e)

        if len(failed_uploads) == 0:
            print('DATA: Todos os leads foram carregados com sucesso')
        else:
            print(f'DATA: {len(failed_uploads)} falharam ao ser carregados')

def filter_companies(elastic: Elasticsearch, companies: list) -> list:
    """Filtra apenas as companhias ativas.
    
        'situacao_cadastral': 'ATIVA'

    A pesquisa do campo situacao_cadastral é feita de forma paralela, para realizar mais rapidamente.

    :param: elastic:Elasticsearch: A conexão com o elastic.
    :param companies:list: A lista de empresas que desejamos filtrar.
    :returns list: A lista de empresas ativas.
    """
    # Primeiro, dividimos a lista de empresas em várias listas
    companies_matrix = divide_list(companies, 1_000)

    # Depois, executamos o filtro paralelamente.
    # threads = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for _companies in companies_matrix:
            # thread = threading.Thread(target=_filter_companies, args=(_companies,))
            # thread.start()
            # threads.append(thread)
            future = executor.submit(_filter_companies, elastic, _companies)
            futures.append(future)

        # Agora vamos juntar todos os resultados em apenas uma lista.
        # Cada resultado é uma lista de empresas ativas, então juntamos tudo para ter a lista
        # completa.
        results = []
        for future in futures:
            result = future.result()
            results.extend(result)

    print('DATA: Aqui')
    return results

def _filter_companies(elastic: Elasticsearch, companies: list) -> list:
    """Filtra apenas as companhias ativas.
    
        'situacao_cadastral': 'ATIVA'

    :param: elastic:Elasticsearch: A conexão com o elastic.
    :param companies:list: A lista de empresas que desejamos filtrar.
    :returns list: A lista de empresas ativas.
    """
    # Primeiro pegamos todos os CNPJs da base nova e pesquisamos no elastic
    cnpjs = [company['_source']['cnpj'] for company in companies]
    try:
        result = elastic.mget(body={'ids': cnpjs},
                              index='empresasdobrasilv12',
                              _source=['cnpj', 'situacao_cadastral'])
    except Exception as e:
        print('DATA: Algo deu errado ao pesquisar no elastic')
        print('DATA:', e)

    result = result['docs']
    # Depois, filtramos apenas os CNPJs que tiverem a situação cadastral ATIVA.
    print(result[0])
    filtered_cnpjs = [company['_source']['cnpj'] for company in result
                                            if company['found']
                                            and company['_source']['situacao_cadastral'] == 'ATIVA']

    print('DATA: Aqui')
    # Finalmente, filtramos a base de leads com base na lista de CNPJs do elastic que tem a
    # situação cadastral ativa.
    filtered_companies = filter(lambda company: company['_source']['cnpj'] in filtered_cnpjs, companies)

    return list(filtered_companies)

# def upload_leads(index_name: str, leads: list) -> None:
#     """Envia o lead criado para o elastic.
    
#     Realiza o envio de forma paralela para ser mais rápido.

#     :param index_name:str: O nome do index criado para a base recebida.
#     :param leads:list: A lista de documentos (leads) a ser enviada para o elastic.

#     :returns None:
#     """
#     leads_matrix = divide_list(leads, 1_000)
#     # n_threads = 5
#     # threads = []

#     _upload_leads(leads_matrix)

#     # leads_tensor = divide_m_parts(leads_matrix, n_threads)
#     # for i in range(n_threads):
#     #     thread = threading.Thread(target=_upload_leads, args=(leads_tensor[i],))
#     #     thread.start()
#     #     threads.append(thread)

#     # for thread in threads:
#     #     thread.join()

# def _upload_leads(leads_matrix: list):
#     """Realiza o upload dos leads passados."""
#     with Elasticsearch([os.environ['ELASTIC_URL']]) as elastic:
#         failed_uploads = []
        
#         for leads_list in leads_matrix:
#             filtered_leads = filter_companies(elastic, leads_list)

#         try:
#             for success, info in helpers.parallel_bulk(elastic, filtered_leads):
#                 pass

#         except Exception as e:
#             print('DATA: [ _upload_leads ] Aconteceu algo de errado ao subir a base')
#             print('DATA: [ _upload_leads ]', e)

# def filter_companies(elastic: Elasticsearch, companies: list) -> list:
#     """Filtra apenas as companhias ativas.
    
#         'situacao_cadastral': 'ATIVA'

#     :param: elastic:Elasticsearch: A conexão com o elastic.
#     :param companies:list: A lista de empresas que desejamos filtrar.
#     :returns list: A lista de empresas ativas.
#     """
#     # Primeiro pegamos todos os CNPJs da base nova e pesquisamos no elastic
#     cnpjs = [company['_source']['cnpj'] for company in companies]
#     result = elastic.mget(body={'ids': cnpjs},
#                             index='empresasdobrasilv10',
#                             _source=['cnpj', 'situacao_cadastral'])
#     result = result['docs']
#     # Depois, filtramos apenas os CNPJs que tiverem a situação cadastral ATIVA.
#     filtered_cnpjs = [company['_source']['cnpj'] for company in result
#                                             if company['_source']['situacao_cadastral'] == 'ATIVA']

#     # Finalmente, filtramos a base de leads com base na lista de CNPJs do elastic que tem a
#     # situação cadastral ativa.
#     filtered_companies = filter(lambda company: company['_source']['cnpj'] in filtered_cnpjs, companies)

#     return list(filtered_companies)

def divide_m_parts(l: list, m: int):
    """Divide uma lista em M partes.
    
    :param l:list: A lista a ser dividida.
    :param m:int: O número de partes a dividir a lista
    """
    n_elements = len(l) // m
    divided_list = divide_list(l, n_elements)
    return divided_list

def divide_list(l, n_elements):
    """Divide uma lista em listas com até n elementos. Isso servirá para não sobrecarregar a consulta do elastic.
    Todas as requisições ao elastic não terão mais de N elementos, então dividiremos a lista
    recebida como parâmetro em M partes, cada uma com N elementos.

    :param l:list: A lista a ser dividida.
    :param n_elements:int: A quantidade máxima de elementos para cada parte.
    
    :returns divisions:list: Uma lista com as divisões (lista de listas), cada uma com N elementos.
    """
    divisions = []
    for i in range(0, len(l), n_elements):
        if i + n_elements < len(l):
            divisions.append(l[i:i+n_elements])
        else:
            divisions.append(l[i:])
    return divisions
