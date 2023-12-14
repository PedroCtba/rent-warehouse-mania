# %% Import
import duckdb

# %% Conexão
con = duckdb.connect(database="C:\\Users\\PedroMiyasaki\\OneDrive - DHAUZ\\Área de Trabalho\\Projetos\\PESSOAL\\rent-warehouse-mania\\db\\rent_warehouse_mania.duckdb")

# %% Definir query SQL
sql_query = """
"""

# %% Executar query
print(con.execute(sql_query).fetchall())