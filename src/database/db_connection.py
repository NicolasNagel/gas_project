import os

from typing import Optional, List, Dict

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from dotenv import load_dotenv

from src.database.db_model import Base, PocosTable, EquipamentosTable, ProducaoTable, IncidentesTable

load_dotenv()

class GasDataBase():
    """Classe de Banco de Dados que tem como responsabilidade
       toda a orquestração do Banco de Dados."""
    def __init__(self):
        
        self.db_user = os.getenv('DB_USER')
        self.db_pass = os.getenv('DB_PASS')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')

        self.engine = create_engine(f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}")
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.Base = Base

        self.Base.metadata.create_all(bind=self.engine)

        self.orm_mapping = {
            "raw_pocos": PocosTable,
            "raw_equipamentos": EquipamentosTable,
            "raw_producao": ProducaoTable,
            "raw_incidents": IncidentesTable
        }

    def check_table_db(self, table_name: Optional[List[str]] = None) -> Dict[str, bool]:
        """Checa se a tabela existe no Banco de Dados, se existe, retorna uma lista, senão, retorna None.
           
           Lista de tabelas a serem verificadas:
           - raw_pocos: Tabela de Cadastro de Poços
           - raw_equipamentos: Tabela de Cadastro de Equipamentos
           - raw_producao: Tabela de Registros de Produção.
           - raw_incidentes: Tabela de Registros de Incidentes.
        
        Args:
            table_name (Optional[str]): Nome da tabela a ser passada para validação,
            se nenhuma for passada, verifica todas.

        Returns:
            Dict[str, bool]: Dicionário com a verificação se existem as tabelas no Banco de Dados.
        """
        