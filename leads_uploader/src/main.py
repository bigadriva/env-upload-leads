import os
import sys
import time

from elasticsearch import Elasticsearch

from config import (
    create_mapping_from_csv,
    index_exists_for_company,
    create_index,
    upload_all_leads
)


def main():
    """Executada na criação do processo pelo resolver de upload do JavaScript.
    Há um resolver no diretório da Lidia do código em JavaScript que cria um processo que executa
    esse script.
    Na criação do processo, o comando chamado é
    
        python config.py filename
    
    Onde filename é o nome do arquivo onde se salvou o arquivo passado pelo front."""

    # time.sleep(30)

    filename = ''
    company_name = ''
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


if __name__ == '__main__':
    main()