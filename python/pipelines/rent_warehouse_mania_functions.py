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
def get_rent_price(possible_prices: list, max_rent: float = float("inf")) -> float:
    # Fazer lista vazia para guardar os valores que realmente podem ser o preço
    new_possible_prices = []

    # Iterar as possíveis strings que contem o preço
    for price in possible_prices:
        # Se a string iterada tiver qualquer dígito numérico
        if any(l.isdigit() for l in price):
            # Mantenha apenas os numéricos ou ".", "," na string
            price = "".join([l for l in price if l.isdigit() or l in (".", ",")])

            # Em casos de preço com virgula, pegue a primeira parte do preço apenas
            price = price.split(",")[0]

            # Remova . se eles forem começo ou final de string
            price = price.strip(".")

            # Converta . em _
            price = price.replace(".", "_")
            
            # Tente
            try:
                # Conversão para floar
                price = float(price)

                # Se o preço for maior que 100_000, divida-o por esse valor
                if price > 99_999:
                    price = price / 100_000

                # Guarde ela na lista 
                new_possible_prices.append(price)

            # Em caso de erro de tipo na conversão, pule essa string
            except ValueError:
                print(f"String -> {price} não é um preço de imóvel!")

    # Filtrar alugueis abaixo de max_rent
    new_possible_prices = [price for price in new_possible_prices if price < max_rent]

    # Retorne o maior campo da lista
    return max(new_possible_prices)

# Fazer função de pegar o tamanho do imóvel
def get_rent_size(possible_sizes: list,  max_size: int = float("inf")) -> float:
    # Fazer lista vazia para guardar os valores que realmente podem ser o tamanho do imóvel
    new_possible_sizes = []

    # Iterar as possíveis strings que contem o tamanho
    for size in possible_sizes:
        # Se a string iterada tiver qualquer dígito numérico que não seja ², ³
        if any(l.isdigit() and l != "²" and l != "³" for l in size):
            # Deixe somente numeros e não deixe ², ³
            size = "".join([l for l in size if (l.isdigit() and l != "²" and l != "³") or (l == ",")])

            # Divida o tamanho na "," (se tiver) e pegue o primeiro campo
            size = size.split(",")[0]

            # Tente
            try:
                # Conversão para inteiro
                size = int(size)
            
                # Guarde ela na lista
                new_possible_sizes.append(size)

            # Em caso de erro de tipo na conversão, pule essa string
            except ValueError:
                print(f"String -> {size} não é um tamanho de imóvel!")

    # Filtrar tamanhnos abaixo de max_size
    new_possible_sizes = [size for size in new_possible_sizes if size < max_size]

    # Retorne o primeiro campo da lista
    return new_possible_sizes[0]

# Fazer função de pegar o endereço do imóvel
def get_rent_adress(rent_splited_words: list) -> str:
    # Importar objetos   
    from rent_warehouse_mania_pipeline_objects import street_synonyms, city_names
    
    # Faça uma lista vazia para guardar os possíveis indexes
    possible_rua_index = []
    possible_city_index = []

    # Iterar todas as palavras buscando os possíveis indices da rua
    for iword, word in enumerate(rent_splited_words):
        # Se a palavra estiver na lista de sinonimos para rua, guarde esse index
        if any(i in word.lower() for i in street_synonyms):
            possible_rua_index.append(iword)

        # Se a palavra estiver na lista de cidades, guarde esse index
        if any(i in word.lower() for i in city_names):
            possible_city_index.append(iword)

    # Se ambas as listas tiverem idnex
    if len(possible_rua_index) != 0 and len(possible_city_index) != 0:
        # Retornar o join de todas as palavras na string, escolhendo o min(possible_rua_index) até o max(possible_city_index)
        return " ".join(rent_splited_words[min(possible_rua_index):max(possible_city_index)])
    
    # Do contrário
    else:
        # Retorne um join geral
        return " ".join(rent_splited_words)
