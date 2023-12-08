# Imports online
import dlt
import requests
from bs4 import BeautifulSoup
from hashlib import md5

# Import offline
from rent_warehouse_mania_pipeline_objects import street_synonyms, city_names

# Fazer função que "yield" o dict com os dados de imóveis
@dlt.resource(name="chaves_na_mao_table", write_disposition="append", primary_key="id")
def get_rents_chaves_na_mao(
    url: str = "https://www.chavesnamao.com.br/imoveis-para-alugar/pr-curitiba/",
    rent_html_class: str = "imoveis__Card-obm8pe-0 tNifl"
) -> dict:
    # Fazer função de filtrar lista de palavras
    def filter_words(words_list, desired_c=None, not_desired_c=None):
        # Retornar lista com as palavras contenham todos os desired_c e nenhum dos not_desired_c
        if desired_c and not_desired_c:
            return [word for word in words_list if any(char in word for char in desired_c) and all(char not in word for char in not_desired_c)]
        
        # Retornar lista com as palavras contenham todos os desired_c
        elif desired_c:
            return [word for word in words_list if any(char in word for char in desired_c)]

        # Retornar lista com as palavras não contenham not_desired_c
        elif not_desired_c:
            return [word for word in words_list if all(char not in word for char in not_desired_c)]

    # Fazer função de pegar o possível preço do imóvel em uma lista
    def get_rent_price(possible_prices: list) -> float:
        # Fazer lista vazia para guardar os valores que realmente podem ser o preço
        new_possible_prices = []

        # Iterar as possíveis strings que contem o preço
        for price in possible_prices:
            # Se a string iterada tiver qualquer dígito numérico
            if any(l.isdigit() for l in price):
                # Mantenha apenas os numéricos ou ".", "," nela
                price = "".join([l for l in price if l.isdigit() or l in (".", ",")])

                # Divida ela na virgula e pegue a primeira parte
                price = price.split(",")[0]
                
                # Tente convertela para float e guarde-a na lista em seguida
                try:
                    price = float(price)

                    # Guarde ela na lista 
                    new_possible_prices.append(price)

                # Em caso de erro de tipo na conversão, pule essa string
                except ValueError:
                    print(f"String -> {price} não é um preço de imóvel!")

        # Retorne o maior campo da lista
        return max(new_possible_prices)

    # Fazer função de pegar o tamanho do imóvel
    def get_rent_size(possible_sizes: list) -> float:
        # Fazer lista vazia para guardar os valores que realmente podem ser o tamanho do imóvel
        new_possible_sizes = []

        # Iterar as possíveis strings que contem o tamanho
        for size in possible_sizes:
            # Se a string iterada tiver qualquer dígito numérico
            if any(l.isdigit() and l != "²" for l in size):
                # Guarde ela na lista
                new_possible_sizes.append(size)

        # Transforme todos os items em "new_possible_sizes" em inteiros
        new_possible_sizes = [int("".join([l for l in price if l.isdigit()]).replace("²", "")) for price in new_possible_sizes]

        # Retorne o primeiro campo da lista
        return new_possible_sizes[0]

    # Fazer função de pegar o endereço do imóvel
    def get_rent_adress(rent_splited_words: list) -> str:
        # Definir o primeiro index como 0 e o final como len(rent_splited_words) por padrão
        rua_index = 0
        city_index = len(rent_splited_words) - 1

        # Iterar todas as palavras
        for iword, word in enumerate(rent_splited_words):
            # Se a palavra estiver na lista de sinonimos para rua, guarde esse index
            if word.lower() in street_synonyms:
                rua_index = iword

            # Se a palavra estiver na lista de cidades, guarde esse index
            if word.lower() in city_names:
                city_index = iword

        # Retornar o join de todas as palavras em styring unica de rua_index até city_index
        return " ".join(rent_splited_words[rua_index:city_index])

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
            preco = get_rent_price(filter_words(imovel_words, desired_c=".", not_desired_c="²"))

            # Pegar campo de tamanho
            tamanho = get_rent_size(filter_words(imovel_words, desired_c="²", not_desired_c="$"))

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