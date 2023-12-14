import duckdb

# Criar a conexão
con = duckdb.connect(database="C:\\Users\\PedroMiyasaki\\OneDrive - DHAUZ\\Área de Trabalho\\Projetos\\PESSOAL\\rent-warehouse-mania\\db\\rent_warehouse_mania.duckdb")

# Pegar todos os esquemas que não seham stagging
schemas = [schema[1] for schema in con.execute("SHOW ALL TABLES").fetchall() if "staging" not in schema[1]]

# Tirar schemas duplicados
not_duplicated_schemas = list(dict.fromkeys(schemas))

# Fazer um dataframe para cada schema
dataframes = []

# Itere todos os esquemas
for schema in not_duplicated_schemas:
    # Set a conexão nesse schema
    con.execute(f"SET schema '{schema}'")

    # Faça uma lista de tabelas desse schema
    schema_tables = [table[0] for table in con.execute("SHOW TABLES").fetchall()]

    # Itere todas as tabelas
    for table in schema_tables:
        # Se a table não for nativa do DLT
        if "dlt" not in table:
            # De um select * na tabela (transforme em dataframe)
            df = con.execute(f"SELECT * FROM {table}").df()

            # Salve na lista
            dataframes.append(df)