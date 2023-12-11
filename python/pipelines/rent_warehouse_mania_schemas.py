# Import pydantic
from pydantic import BaseModel

# Contruir classe schema de imovel
class Imovel(BaseModel):
    id: str
    preco: float
    tamanho: int
    endereco: str