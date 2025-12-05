# models.py
from sqlalchemy import Column, Integer, String, Float
from database import Base

class UnidadeComercial(Base):
    __tablename__ = "unidades_comerciais"

    id = Column(Integer, primary_key=True, index=True)

    # Dados originais do Excel (Colunas do "Tabela_UC.xlsx - data.csv")
    rede = Column(String)                   # Coluna 'Rede' -> Para o Cluster
    nome = Column(String)                   # Coluna 'Nome'
    endereco_original = Column(String)      # Coluna 'Endereço' -> Para Geocode
    cnpj = Column(String)                   # Coluna 'CNPJ/CPF'
    
    # Dados de geocodificação
    endereco_usado_geocode = Column(String) # Endereço que funcionou (Debug)
    latitude = Column(Float)
    longitude = Column(Float)