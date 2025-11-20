from typing import Optional, Dict
from datetime import datetime
import pandas as pd

from sqlalchemy import text

from src.database.db_connection import GasDataBase
from src.data.generate_fake_data import FakeData
from src.schemas.schema_validacao import ValidateSchema

class PipelineController():
    """
    Controlador principal do pipeline de dados.

    Responsável por orquestrar:
    1. Geração de dados fake
    2. Validação com Pandera
    3. Inserção no Banco de Dados
    4. Relatórios de Execução.
    """
    def __init__(self, db_connection: Optional[GasDataBase] = None):
        """
        Inicializa o controller do Pipeline.

        Args:
            db_connection: Conexão com o Banco de Dados. Se none, cria uma nova.
        """
        self.db = db_connection if db_connection else GasDataBase()
        self.generator = FakeData(db_connection=self.db)
        self.validador = ValidateSchema()

        self.execution_log = {
            'start_time': None,
            'end_time': None,
            'status': 'not_started',
            'tables_generated': {},
            'tables_verified': {},
            'tables_inserted': {},
            'errors': []
        }

    def run_full_pipeline(
        self,
        lotes: Optional[Dict[str, int]] = None,
        skip_validation: bool = False
    ) -> Dict[str, any]:
        """
        Executa o pipeline completo: gerar dados -> validar dados -> inserir dados.

        Args:
            lotes (Optional[Dict[str, any]]): Dicionário com os tamanhos de lote por tabela.
                                              Se None, usa os valores padrões.
            skip_validation: Se True, pula validação Pandera (não recomendado).

        Returns:
            Dict com relatório de Execução.

        Example:
            >>> controller = PipelineController()
            >>> resultado = controller.run_full_pipeline(lotes={
            ...     'pocos': 100,
            ...     'equipamentos': 500,
            ...     'producao': 2000,
            ...     'incidentes': 250
            ... })
        """
        try:
            self._log_start()

            df = self._generate_data(lotes)
            if not df:
                raise ValueError("Nenhum dado foi gerado.")
            
            if not skip_validation:
                df = self._validate_data(df)
            else:
                print(f"Validação Pulada (skip_validation=True)")

            resultado_insercao = self._insert_data(df)
            self._log_end(status='success')
            self._print_summary()

            return self.execution_log
        
        except Exception as e:
            self._log_end(status='failed', error=str(e))
            print(f"Pipeline falhou: {e}")
            raise

    def _generate_data(self, lotes: Optional[Dict[str, int]] = None) -> Dict[str, pd.DataFrame]:
        """
        Gera dados fake para todas as tabelas.

        Args:
            lotes (Optional[Dict[str, int]]): Tamanho de lote personalizados.

        Returns:
            Dict com DataFrames gerados.
        """
        default_lotes = {
            'pocos': 100,
            'equipamentos': 500,
            'producao': 2000,
            'incidentes': 250
        }

        lotes = lotes if lotes else default_lotes
        df = {}

        try:
            print(f"Gerando {lotes.get('pocos', 100)} poços...")
            df_pocos = self.generator.generate_pocos_table(
                tamanho_lote=lotes.get('pocos', 100)
            )

            if df_pocos.empty:
                raise ValueError("Falha ao gerar poços")
            
            df['raw_pocos'] = df_pocos
            self.execution_log['tables_generated']['raw_pocos'] = len(df_pocos)

            print(f"\nGerando {lotes.get('equipamentos', 500)} equipamentos...")
            df_equipamentos = self.generator.generate_equipamentos_table(
                tamanho_lote=lotes.get('equipamentos', 500),
                df_pocos=df_pocos
            )

            if df_equipamentos.empty:
                raise ValueError("Falha ao gerar equipamentos")
            
            df['raw_equipamentos'] = df_equipamentos
            self.execution_log['tables_generated']['raw_equipamentos'] = len(df_equipamentos)

            print(f"\nGerando {lotes.get('producao', 2000)} registros de produção...")
            df_producao = self.generator.generate_producao_table(
                tamanho_lote=lotes.get('producao', 2000),
                df_pocos=df_pocos
            )

            if df_producao.empty:
                raise ValueError("Falha ao gerar registros de produção.")
            
            df['raw_producao'] = df_producao
            self.execution_log['tables_generated']['raw_producao'] = len(df_producao)

            print(f"\nGerando {lotes.get('incidentes', 250)} incidentes...")
            df_incidentes = self.generator.generate_incidentes_table(
                tamanho_lote=lotes.get('incidentes', 250),
                df_equipamentos=df_equipamentos,
                df_producao=df_producao
            )

            if df_incidentes.empty:
                raise ValueError("Falha ao gerar registros de incidentes.")
            
            df['raw_incidentes'] = df_incidentes
            self.execution_log['tables_generated']['raw_incidentes'] = len(df_incidentes)

            total_gerado = sum(len(dfs) for dfs in df.values())
            print(f"GERAÇÃO CONCLUÍDA: {total_gerado} registros no total")

            return df
        
        except Exception as e:
            self.execution_log['errors'].append(f"Erro na geração:{e}")
            raise

    def _validate_data(self, df: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Valida todos os DataFrames com Pandera.

        Args:
            df (Dict[str, DataFrame]): DataFrames para validar.

        Returns:
            DataFrames validados com Pandera.
        """
        validated = {}
        try:
            if 'raw_pocos' in df:
                print(f"\nValidando poços...")
                validated['raw_pocos'] = self.validador.validate_pocos_table(
                    df_pocos=df['raw_pocos']
                )
                self.execution_log['tables_verified']['raw_pocos'] = len(validated['raw_pocos'])

            if 'raw_equipamentos' in df:
                print(f"\nValidando equipamentos...")
                validated['raw_equipamentos'] = self.validador.validate_equipamentos_table(
                    df['raw_equipamentos']
                )
                self.execution_log['tables_verified']['raw_equipamentos'] = len(validated['raw_equipamentos'])

            if 'raw_producao' in df:
                print(f"\nValidando registros de produção...")
                validated['raw_producao'] = self.validador.validate_producao_table(
                    df['raw_producao']
                )
                self.execution_log['tables_verified']['raw_producao'] = len(validated['raw_producao'])

            if 'raw_incidentes' in df:
                print(f"\nValidando registros de incidentes...")
                validated['raw_incidentes'] = self.validador.validate_incidentes_table(
                    df['raw_incidentes']
                )
                self.execution_log['tables_verified']['raw_incidentes'] = len(validated['raw_incidentes'])

            total_validado = sum(len(dfs) for dfs in validated.values())
            print(f"\nVALIDAÇÃO CONCLUÍDA: {total_validado} registros validados.")

            return validated
         
        except Exception as e:
            self.execution_log['errors'].append(f"Erro na validação: {e}")
            raise

    def _insert_data(self, df: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """
        Insere DataFrames no Banco de Dados.

        Args:
            df (Dict[str, DataFrame]): DataFrames validados para inserção.

        Returns:
            Dict com a quantidade inserida por tabela.
        """
        try:
            resultado = self.db.insert_values_into_db(data=df)

            self.execution_log['tables_inserted'] = resultado

            total_inserido = sum(resultado.values())
            print(f"INSERÇÃO CONCLUÍDA: {total_inserido} registros inseridos")

            return resultado

        except Exception as e:
            self.execution_log['errors'].append(f"Erro na inserção: {e}")
            raise

    def _log_start(self):
        """Registra o inicio da execução."""
        self.execution_log['start_time'] = datetime.now()
        self.execution_log['status'] = 'running'

        print(f"\n" + "=" * 70)
        print(f"INICIANDO PIPELINE DE DADOS")
        print("=" * 70)
        print(f"Inicio: {self.execution_log['start_time'].strftime('%d/%m/%Y %H:%M:%S')}")

    def _log_end(self, status: str, error: Optional[str] = None):
        """Registro o fim da execução."""
        self.execution_log['end_time'] = datetime.now()
        self.execution_log['status'] = status

        if error:
            self.execution_log['errors'].append(error)

    def _print_summary(self):
        """Imprime o resumo da execução."""
        duration = (
            self.execution_log['end_time'] -
            self.execution_log['start_time']
        ).total_seconds()

        print("\n" + "=" * 70)
        print("RESUMO DA EXECUÇÃO")
        print("=" * 70)

        print(f"\n Duração: {duration:.2f} segundos")
        print(f"Status: {self.execution_log['status'].upper()}")

        print(f"\nREGISTROS GERADOS:")
        for table, count in self.execution_log['tables_generated'].items():
            print(f"°{table}: {count}")

        print(f"\nREGISTROS VALIDADOS:")
        for table, count in self.execution_log['tables_verified'].items():
            print(f"°{table}: {count}")

        print(f"\nREGISTROS INSERIDOS:")
        for table, count in self.execution_log['tables_inserted'].items():
            print(f"°{table}: {count}")
        
        if self.execution_log['errors']:
            print(f"\nERROS:")
            for error in self.execution_log['errors']:
                print(f"°{error}")

        total_inserido = sum(self.execution_log['tables_inserted'].values())
        print(f"\nPIPELINE CONCLUÍDA COM SUCESSO!")
        print(f"    Total de {total_inserido} registros inseridos no Banco de Dados.")
        print("=" * 70)

    def check_database_status(self) -> Dict[str, int]:
        """Verifica status atual do Banco de Dados.
        
        Returns:
            Dicionário com a contagem de registros por tabela.
        """
        print("\n" + "=" * 70)
        print(f"VERIFICANDO STATUS DO BANCO DE DADOS")
        print("=" * 70)

        status = self.db.check_table_values_into_db()

        print(f"\nResumo:")
        for table, count in status.items():
            print(f"° {table}: {count} registros")

        total = sum(status.values())
        print(f"\n  Total: {total} registros no Banco de Dados")

        return status
    
    def truncate_all_tables(self, confirm: bool = False):
        """
        CUIDADO: Limpa todas as tabelas do Banco.

        Args:
            confirm: Deve ser True para executar (segurança).

        Example:
            >>> # Limpar todas as tabelas
            >>> controller.truncate_all_tables(confirm=True)
        """
        if not confirm:
            print(f"Para limpar as tabelas, passe confirm=True")
            return
        
        print("\n" + "=" * 70)
        print("LIMPANDO TODAS AS TABELAS")
        print("="  *70)

        tabelas = ['raw_incidentes', 'raw_producao', 'raw_equipamentos', 'raw_pocos']

        with self.db.SessionLocal() as session:
            try:
                for tabela in tabelas:
                    print(f"    Limpando {tabela}...")
                    session.execute(text(f"TRUNCATE TABLE {tabela} CASCADE"))

                session.commit()
                
                print(f"\nTodas as tabelas foram limpas.")
            except Exception as e:
                session.rollback()
                print(f"\nErro ao limpar tabelas: {e}")
                raise