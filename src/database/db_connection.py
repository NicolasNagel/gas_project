import os
import pandas as pd

from typing import Optional, List, Dict, Set

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
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
            "raw_incidentes": IncidentesTable,
        }

    def check_tables_into_db(self, table_name: Optional[List[str]] = None) -> List[str]:
        """Checa se a tabela existe no Banco de Dados, se existe, retorna uma lista, senão, retorna None.
           
        Args:
            table_name (Optional[str]): Lista de nomes das tabelas. Se None, verifica todas

        Returns:
            List[str]: Lista das tabelas verificadas no Banco de Dados.

        Raises:
            Exeception: Erro crítico ao tentar validar as tabelas.
        """
        try:
            if table_name is None:
                print("Nenhuma tabela foi passada, verificando todas...")
                tabelas_projeto = list(self.orm_mapping.keys())
            else:
                print(f"{len(table_name)} Tabelas para serem verificadas, iniciando...")
                tabelas_projeto = table_name

            print(f"{len(tabelas_projeto)} Tabelas verificadas no Banco de Dados.")
            return tabelas_projeto

        except Exception as e:
            print(f"Erro crítico em check_table_db: {str(e)}")
            raise

    def check_table_values_into_db(self, table_name: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Verifica se existem registros na tabela, se existir, retorna eles, senão None.

        Args:
            table_names (Optional[List[str]]): Lista de tabelas que serão verificados os registros.

        Returns:
            Dict[str, int]: Dicionário com o nome da tabela no Banco de Dados e a quantidade de registros em cada uma.

        Raises:
            Exception: Erro crítico ao checar a quantidade de registros no Banco de Dados.

        Example:
            >>> db.check_table_values_into_db()
            {'raw_pocos': 150, 'raw_equipamentos': 500, ...}
        """
        try:
            if table_name is None:
                print(f"Nenhuma tabela foi passada, verificando o registro de todas as tabelas.\n")
                tabelas_projeto = list(self.orm_mapping.keys())
            else:
                print(f"{len(table_name)} Tabelas para serem verificados os registros.\n")
                tabelas_projeto = table_name

            resultado = {}

            with self.SessionLocal() as session:
                for tabelas in tabelas_projeto:

                    orm_class = self.orm_mapping.get(tabelas)
                    if orm_class is None:
                        print(f"A Tabela: {tabelas} está vazia.")
                        resultado[tabelas] = 0
                        continue

                    try:
                        contagem = session.query(orm_class).count()
                        resultado[tabelas] = contagem
                        if contagem == 0:
                            print(f"{tabelas}: 0 registros (vazia)")
                        else:
                            print(f"{tabelas}: {contagem} registros")

                    except Exception as e:
                        print(f"Erro ao contar {tabelas}: {str(e)}")
                        resultado[tabelas] = 0

            total_registros = sum(resultado.values())
            print(f"\nTotal: {total_registros} registros em {len(resultado)} tabela(s)")

            return resultado

        except Exception as e:
            print(f"Erro crítico em check_tables_values_into_db: {str(e)}")
            raise

    def insert_values_into_db(
            self,
            data: Optional[Dict[str, pd.DataFrame]] = None,
        ) -> Dict[str, int]:
        """
        Faz a inserção de dados das tabelas no Banco de Dados apartir de DataFrames validados.
        
        Args:
            data (Optional[Dict[str, DataFrame]]): Dicionário com {nome_tabela: DataFrame}

        Returns:
            Dict[str, int]: {nome_tabela: quantidade_inserida}.

        Raises:
            Exeception: Erro crítico ao fazer a inserção de dados nas tabelas.

        Example:
            >>> db.insert_values_into_db(data={
            ...     'raw_pocos': df_pocos_validados,
            ...     'raw_equipamentos': df_equipamentos_validado
            ...})
            {'raw_pocos': 150, 'raw_equipamentos': 500}
        """
        try:
            if data is not None:
                print(f"Inserindo dados em {len(data)} tabelas")
                dados_para_inserir = data

            for nome_tabela, df in dados_para_inserir.items():
                if df is None or df.empty:
                    print(f"{nome_tabela}: DataFrame vazio, pulando...")
                    dados_para_inserir[nome_tabela] = None

            resultado = {}
            with self.SessionLocal() as session:
                try:
                    for nome_tabela, df in dados_para_inserir.items():
                        if df is None:
                            resultado[nome_tabela] = 0
                            continue

                        orm_class = self.orm_mapping.get(nome_tabela)
                        if orm_class is None:
                            print(f"{nome_tabela}: Tabela não encontrada no mapeamento.")
                            resultado[nome_tabela] = 0
                            continue

                        print(f"Inserindo em {nome_tabela}...")

                        records = df.to_dict('records')
                        session.bulk_insert_mappings(orm_class, records)

                        quantidade = len(records)
                        resultado[nome_tabela] = quantidade

                        print(f"{quantidade} de registros inseridos")

                    session.commit()
                
                except IntegrityError as e:
                    session.rollback()
                    print(f"Erro de integridade (chave_duplicada?): {e.orig}")
                    raise

                except Exception as e:
                    session.rollback()
                    print(f"Erro durante a inserção: {str(e)}")

            total_inserido = sum(resultado.values())
            print(f"\nTotal: {total_inserido} registros inseridos")

            return resultado

        except Exception as e:
            print(f"Erro crítico em insert_values_into_db: {str(e)}")
            raise

    def get_existing_codes(
            self,
            table_name: str,
            code_column: str
        ) -> Set[str]:
        """
        Busca todos os códigos únicos já existentes em uma tabela.

        Args:
            table_name: Nome da tabela no mapeamento ORM.
            code_column: Nome da Coluna que contém o código único.

        Returns:
            Set[str]: Conjunto com todos os códigos existentes.

        Example:
            >>> db.get_exisiting_codes('raw_pocos', 'codigo_poco')
            {'POCO_001', 'POCO_002', 'POCO_003', ...}
        """
        try:
            orm_class = self.orm_mapping.get(table_name)
            if orm_class is None:
                print(f"Tabela {table_name} não encontrada no mapeamento")
                return set()
            
            if not hasattr(orm_class, code_column):
                print(f"Coluna {code_column} não existe na tabela {table_name}")
                return set()
            
            with self.SessionLocal() as session:
                column = getattr(orm_class, code_column)
                results = session.query(column).all()
                codigos_existentes = {row[0] for row in results}

                print(f"{len(codigos_existentes)} código(s) existente(s) em {table_name}")
                return codigos_existentes
            
        except Exception as e:
            print(f"Erro crítico em get_existing_codes: {str(e)}")