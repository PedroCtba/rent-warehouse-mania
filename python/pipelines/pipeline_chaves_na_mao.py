# Imports online
import dlt
from dlt.sources.helpers import requests
from bs4 import BeautifulSoup
from hashlib import md5

# Imports offline
from rent_warehouse_mania_schemas import Imovel
from rent_warehouse_mania_functions import filter_words, get_rent_price, get_rent_size, get_rent_adress

# Fazer função que "yield" o dict com os dados de imóveis
@dlt.resource(name="chaves_na_mao_table", write_disposition="append", primary_key="id", columns=Imovel)
def get_rents_chaves_na_mao(
    url: str = "https://www.chavesnamao.com.br/imoveis-para-alugar/pr-curitiba/",
    rent_html_class: str = "imoveis__Card-obm8pe-0 tNifl"
) -> dict:
    # Pegar a response
    response = requests.get(url)

    # Se o status vier 200, prossiga
    if response.status_code == 200:
        # Pegar sopa de letras com o BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Pegar todas as divs com a classe rent_html_class
        imoveis = [imovel.text for imovel in soup.find_all("div", class_=rent_html_class)]

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

            # Gerar id com hash md5
            rent_id = md5(endereco.encode("utf-8")).hexdigest()

            # Retornar o dicionários com os dados do imóvel
            yield  {
                "id": rent_id,
                "preco": preco,
                "tamanho": tamanho,
                "endereco": endereco,
            }

# Fazer pipeline DLT
pipeline = dlt.pipeline(
    # Nome do pipeline
    pipeline_name="chaves_na_mao_pipeline",

    # Nome do schema dentro do DB (Nome da tabela definido no decorator)
    dataset_name="chaves_na_mao_schema",

    # Destino duckdb
    destination="duckdb",

    # Caminho do DB
    credentials="../db/rent_warehouse_mania.duckdb"
)

# Executar pipeline
pipeline.run(get_rents_chaves_na_mao)