# Import pydantic
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# Fazer classe imovel
class Register(BaseModel):
    id: str
    datahora: datetime

# Contruir classe schema de imovel register
class ImovelRegister(Register):
    preco: Optional[float]
    tamanho: Optional[int]
    endereco: str
    qtd_quartos: Optional[int]
    qtd_banheiros: Optional[int]
    qtd_vagas: Optional[int]


# Contruir classe schema de imovel History
class PriceRegister(Register):
    preco: Optional[float]