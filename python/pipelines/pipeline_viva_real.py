# Imports online
import dlt
from datetime import datetime
from typing import Iterable
from requests.exceptions import HTTPError
from dlt.sources.helpers import requests
from bs4 import BeautifulSoup
from hashlib import md5

# Imports offline
from resources.rent_warehouse_mania_schemas import ImovelRegister, PriceRegister
from resources.rent_warehouse_mania_functions import filter_words, get_rent_price, get_rent_size, get_rent_adress, get_rent_n_of_mapped_rooms

# Definir limite de paginas para essa fonte
PAGE_NUMBER_LIMIT = 100

# Fazer função para geração do cadastro dos imóveis
@dlt.resource(name="viva_real_register", write_disposition="merge", primary_key="id", columns=ImovelRegister)
def generate_viva_real_register(
    page_number: int = 1,
    base_url: str = "https://www.vivareal.com.br/aluguel/parana/curitiba/?pagina=@#onde=,Paran%C3%A1,Curitiba,,,,,city,BR%3EParana%3ENULL%3ECuritiba,,,",
    rent_html_class: str = "property-card__container js-property-card",
    rent_html_element: str = "article",
) -> Iterable[dict]:
    while True:
        # Definir url pagina atual
        url = base_url.replace("@", str(page_number))

        # Mostra página atual iterada
        print(f"URL Base -> {base_url};\nPágina iterada atualmente -> {page_number}")

        # Tentar pegar a response
        try:
            response = requests.get(url)

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
                preco = get_rent_price(filter_words(imovel_words, desired_c=[".", "$"], not_desired_c=["²", "³"]), max_rent=50_000)

                # Pegar campo de tamanho
                tamanho = get_rent_size(filter_words(imovel_words, desired_c=["²", "³"], not_desired_c="$"), max_size=5_000)

                # Pegar campo de endereço
                endereco = get_rent_adress(imovel_words)

                # Pegar campo de n de quartos
                qtd_quartos = get_rent_n_of_mapped_rooms(imovel_words, mapped_room_name="quarto")

                # Pegar campo de n banheiros
                qtd_banheiros = get_rent_n_of_mapped_rooms(imovel_words, mapped_room_name="banheiro")

                # Pegar campo de n vagas garagem
                qtd_vagas = get_rent_n_of_mapped_rooms(imovel_words, mapped_room_name="vaga")      

                # Gerar id com hash md5
                rent_id = md5(endereco.encode("utf-8")).hexdigest()

                # Retornar o dicionários com os dados do imóvel
                yield  {
                    "id": rent_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": preco,
                    "tamanho": tamanho,
                    "endereco": endereco,
                    "qtd_quartos": qtd_quartos,
                    "qtd_banheiros": qtd_banheiros,
                    "qtd_vagas": qtd_vagas,
                }

            # Incrementar pagina para próximo yield
            page_number += 1

        # Se a função chega no limte de página WEB
        if page_number > PAGE_NUMBER_LIMIT:
            # Mostre para o usuário
            print(f"URL Base -> {base_url};\nO scrapper atingiu o limte de páginas [{PAGE_NUMBER_LIMIT}] web com essa URL base;")

            # Pare o laço
            break

# Fazer função para registro de mudanças de preço dos imóveis
@dlt.resource(name="viva_real_history", write_disposition="append", primary_key="id", columns=PriceRegister)
def generate_viva_real_history(
    page_number: int = 1,
    base_url: str = "https://www.vivareal.com.br/aluguel/parana/curitiba/?pagina=@#onde=,Paran%C3%A1,Curitiba,,,,,city,BR%3EParana%3ENULL%3ECuritiba,,,",
    rent_html_class: str = "property-card__container js-property-card",
    rent_html_element: str = "article",
) -> Iterable[dict]:
    while True:
        # Definir url pagina atual
        url = base_url + f"?pg={page_number}"

        # Mostra página atual iterada
        print(f"URL Base -> {base_url};\nPágina iterada atualmente -> {page_number}")

        # Tentar pegar a response
        try:
            response = requests.get(url)

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
                preco = get_rent_price(filter_words(imovel_words, desired_c=[".", "$"], not_desired_c=["²", "³"]), max_rent=50_000)

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

        # Se a função chega no limte de página WEB
        if page_number > PAGE_NUMBER_LIMIT:
            # Mostre para o usuário
            print(f"URL Base -> {base_url};\nO scrapper atingiu o limte de páginas [{PAGE_NUMBER_LIMIT}] web com essa URL base;")

            # Pare o laço
            break

# Fazer função juntando os recursos do site viva real
@dlt.source
def generate_viva_real():
    # yield resources
    yield generate_viva_real_register
    yield generate_viva_real_history

# Fazer pipeline DLT
pipeline = dlt.pipeline(
    # Nome do pipeline
    pipeline_name="viva_real_pipeline",

    # Nome do schema dentro do DB (Nome da tabela definido no decorator)
    dataset_name="viva_real_schema",

    # Destino duckdb
    destination="duckdb",

    # Caminho do DB
    credentials="../db/rent_warehouse_mania.duckdb"
)

# Executar pipeline com o source
pipeline.run(generate_viva_real())