# Imports DLT
import dlt
from dlt.sources.helpers import requests

# Imports funções
from datetime import datetime
from typing import Iterable
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
from hashlib import md5

# Imports offline
from resources.rent_warehouse_mania_schemas import ImovelRegister, PriceRegister
from resources.rent_warehouse_mania_functions import filter_words, get_rent_price, get_rent_size, get_rent_adress, get_rent_neighborhood

# Fazer função para geração do cadastro dos imóveis
@dlt.resource(name="chaves_na_mao_register", write_disposition="merge", primary_key="id", columns=ImovelRegister)
def generate_chaves_na_mao_register(
    page_number: int = 1,
    base_url: str = "https://www.chavesnamao.com.br/imoveis-para-alugar/pr-curitiba/",
    rent_html_class: str = "imoveis__Card-obm8pe-0 tNifl",
    rent_html_element: str = "div",
) -> Iterable[dict]:
    while True:
        # Definir url pagina atual
        url = base_url + f"?pg={page_number}"

        # Mostra página atual iterada
        print(f"URL Base -> {base_url};\nPágina iterada atualmente -> {page_number}")

        # Tentar pegar a response
        try:
            response = requests.get(url, allow_redirects=False)

        # Em caso de erro, pare a função
        except HTTPError:
            # Mostre a url atual
            print(f"Data scrapper tentou acessar a página [{page_number}] da URL base. \nNão obteve HTML na resposta")
            
            # Pare a função
            break 

        # Se o status vier 200, prossiga
        if response.status_code == 200:
            # Pegar sopa de letras com o BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            # Pegar todas as divs com a classe rent_html_class
            imoveis = [imovel.text for imovel in soup.find_all(rent_html_element, class_=rent_html_class)]

            # Iterar todos os imóveis
            for imovel in imoveis:
                # Splitar palavras do card do imovel
                imovel_words = imovel.split()

                # Pegar campo de preço do imovel
                preco = get_rent_price(filter_words(imovel_words, desired_c=(".", "$"), not_desired_c=("²", "³")), max_rent=50_000)

                # Pegar campo de tamanho
                tamanho = get_rent_size(filter_words(imovel_words, desired_c=("²", "³"), not_desired_c="$"), remove_from_size_chars=("²", "³"), max_size=5_000)

                # Pegar campo de endereço
                endereco = get_rent_adress(imovel_words)

                # Pegar o campo de bairro
                bairro = get_rent_neighborhood(imovel_words)

                # Gerar id com hash md5
                rent_id = md5(endereco.encode("utf-8")).hexdigest()

                # Retornar o dicionários com os dados do imóvel
                yield  {
                    "id": rent_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": preco,
                    "tamanho": tamanho,
                    "endereco": endereco,
                    "bairro": bairro
                }

            # Incrementar pagina para próximo yield
            page_number += 1

        # Se o status não for 200
        else:
            # Mostre a url atual
            print(f"Data scrapper tentou acessar a página [{page_number}] da URL base. \nNão obteve HTML na resposta")
            
            # Pare a função
            break 

# Fazer função para registro de mudanças de preço dos imóveis
@dlt.resource(name="chaves_na_mao_history", write_disposition="append", primary_key="id", columns=PriceRegister)
def generate_chaves_na_mao_history(
    page_number: int = 1,
    base_url: str = "https://www.chavesnamao.com.br/imoveis-para-alugar/pr-curitiba/",
    rent_html_class: str = "imoveis__Card-obm8pe-0 tNifl",
    rent_html_element: str = "div",
) -> Iterable[dict]:
    while True:
        # Definir url pagina atual
        url = base_url + f"?pg={page_number}"

        # Mostra página atual iterada
        print(f"URL Base -> {base_url};\nPágina iterada atualmente -> {page_number}")

        # Tentar pegar a response
        try:
            response = requests.get(url, allow_redirects=False)

        # Em caso de erro, pare a função
        except HTTPError:
            # Mostre a url atual
            print(f"Data scrapper tentou acessar a página [{page_number}] da URL base. \nNão obteve HTML na resposta")
            
            # Pare a função
            break 

        # Se o status vier 200, prossiga
        if response.status_code == 200:
            # Pegar sopa de letras com o BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            # Pegar todas as divs com a classe rent_html_class
            imoveis = [imovel.text for imovel in soup.find_all(rent_html_element, class_=rent_html_class)]

            # Iterar todos os imóveis
            for imovel in imoveis:
                # Splitar palavras do card do imovel
                imovel_words = imovel.split()

                # Pegar campo de preço do imovel
                preco = get_rent_price(filter_words(imovel_words, desired_c=(".", "$"), not_desired_c=("²", "³")), max_rent=50_000)

                # Pegar campo de endereço
                endereco = get_rent_adress(imovel_words)

                # Gerar id com hash md5
                rent_id = md5(endereco.encode("utf-8")).hexdigest()

                # Retornar o dicionários com os dados do imóvel
                yield  {
                    "id": rent_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": preco,
                }

            # Incrementar pagina para próximo yield
            page_number += 1

        # Se o status não for 200
        else:
            # Mostre a url atual
            print(f"Data scrapper tentou acessar a página [{page_number}] da URL base. \nNão obteve HTML na resposta")
            
            # Pare a função
            break 

# Fazer função juntando os recursos do chaves na mão
@dlt.source
def generate_chaves_na_mao():
    # yield resources
    yield generate_chaves_na_mao_register
    yield generate_chaves_na_mao_history

# Fazer pipeline DLT
pipeline = dlt.pipeline(
    # Nome do pipeline
    pipeline_name="chaves_na_mao_pipeline",

    # Nome do schema dentro do DB (Nome da tabela definido no decorator)
    dataset_name="chaves_na_mao_schema",

    # Destino duckdb
    destination="duckdb",

    # Caminho do DB
    credentials="../../db/rent_warehouse_mania.duckdb"
)

# Executar pipeline com o source
pipeline.run(generate_chaves_na_mao())