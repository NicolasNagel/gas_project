import pandas as pd
import pandera.pandas as pa

from typing import Optional
from datetime import date
from pandera.errors import SchemaError


class ValidateSchema():
    """Classe de validção de Schema usando Pandera."""
    def __init__(self):
        pass

    def validate_pocos_table(self, df_pocos: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Valida os dados de Poços Gerados usando Pandera e retorna None caso sucesso.
        
        Args:
            df_pocos (Optional[DataFrame]): DataFrame com os dados de poços gerados para validação.

        Returns:
            DataFrame: Em caso de sucesso, retorna DataFrame validado, senão retorna SchemaError(s).

        Raises:
            ValueError: Se o DataFrame estiver vazio.
            SchemaError: Se validação falhar
        """
        try:
            if df_pocos is None or df_pocos.empty:
                raise ValueError("Erro: Não será possível validar os dados, nenhum DataFrame foi passado.")
            else:
                print(f"{len(df_pocos)} registros para serem validados.")

            schema = pa.DataFrameSchema(
                {
                    "codigo_poco": pa.Column(str, nullable=False, unique=True),
                    "nome_poco": pa.Column(str, nullable=False),
                    "tipo_poco": pa.Column(int, pa.Check.isin([1, 2]), nullable=False),
                    "localizacao": pa.Column(str, nullable=False),
                    "camada": pa.Column(str, pa.Check.isin(['Pre-Sal', 'Pos-Sal']), nullable=False),
                    "profundidade_metros": pa.Column(int, pa.Check.between(500, 7_000), nullable=False),
                    "status_operacional": pa.Column(str, pa.Check.isin(['Ativo', 'Manutenção', 'Inativo']), nullable=False),
                    "data_perfuracao": pa.Column(date, nullable=False),
                    "operadora": pa.Column(str, pa.Check.isin(['Petrobras', 'Shell', 'TotalEnergies', 'Equinor']), nullable=False) 
                }, strict=True
            )
            
            try:
                validated_data = schema.validate(df_pocos, lazy=True)
                print(f"{len(validated_data)} registros validados com sucesso.")

                return validated_data
            
            except SchemaError as e:
                print(f"Erro de validação:\n{str(e)}")
                raise
        
        except Exception as e:
            print(f"Erro crítico em validate_pocos_table: {str(e)}")

    
    def validate_equipamentos_table(self, df_equipamentos: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Valida os dados de Equipamentos gerados usando Pandera e retorna None caso sucesso.
        
        Args:
            df_equipamentos (Optional[DataFrame]): DataFrame com os dados dos Equipamentos para validação.

        Returns:
            DataFrame: Em caso de sucesso, retorna DataFrame validado, senão retorna SchemaError(s).
        
        Raises:
            ValueError: Se o DataFrame estiver vazio.
            SchemaError: Se validação falhar
        """
        try:
            if df_equipamentos is None or df_equipamentos.empty:
                raise ValueError("Erro: Não será possível validar os dados, nenhum DataFrame foi passado.")
            else:
                print(f"{len(df_equipamentos)} registros para serem validados.")

            schema = pa.DataFrameSchema(
                {
                    "cod_equipamento": pa.Column(str, unique=True, nullable=False),
                    "cod_poco": pa.Column(str, nullable=False),
                    "tipo_equipamento": pa.Column(str, pa.Check.isin(["Bomba Submersível", "FPSO", "Válvula DHSV", "Sistema de Elevação", "Compressor", "Separador"]), nullable=False),
                    "marca": pa.Column(str, pa.Check.isin(["Schulemberger", "Haliburton", "Baker Hughes", "Weatherford", "NOV"]), nullable=False),
                    "modelo": pa.Column(str, nullable=False),
                    "data_instalacao": pa.Column(date, nullable=False),
                    "vida_util_anos": pa.Column(int, pa.Check.between(10, 25), nullable=False),
                    "ultimo_teste": pa.Column(date, nullable=False),
                    "eficiencia_operacional": pa.Column(float, pa.Check.between(0.6, 1), nullable=False)
                }, strict=True
            )

            try:
                validated_data = schema.validate(df_equipamentos, lazy=True)
                print(f"{len(validated_data)} registros validados com sucesso.")

                return validated_data
            
            except SchemaError as e:
                print(f"Erro de validação:\n{str(e)}")
                raise
        
        except Exception as e:
            print(f"Erro crítico em validate_equipamentos_table: {str(e)}")
            raise

    def validate_producao_table(self, df_producao: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Valida os dados de Produção gerados usando Pandera e retorna None caso sucesso.
        
        Args:
            df_producao (Optional[DataFrame]): DataFrame com os dados de Produção para validação.

        Returns:
            DataFrame: Em caso de sucesso, retorna DataFrame validado, senão retorna SchemaError(s).
        
        Raises:
            ValueError: Se o DataFrame estiver vazio.
            SchemaError: Se validação falhar
        """
        try:
            if df_producao is None or df_producao.empty:
                raise ValueError("Erro: Não será possível validar os dados, nenhum DataFrame foi passado.")
            else:
                print(f"{len(df_producao)} registros para serem validados.")
            
            schema = pa.DataFrameSchema(
                {
                    "cod_producao": pa.Column(str, unique=True, nullable=False),
                    "cod_poco": pa.Column(str, nullable=False),
                    "data_producao": pa.Column(date, nullable=False),
                    "petroleo_barris_dia": pa.Column(int, pa.Check.between(100, 200_000), nullable=False),
                    "agua_produzida_m3": pa.Column(float, pa.Check.ge(0), nullable=False),
                    "tempo_horas_operacao": pa.Column(float, pa.Check.between(0, 24), nullable=False),
                    "pressao_bar": pa.Column(int, pa.Check.between(150, 450), nullable=False),
                    "temperatura_celsius": pa.Column(float, pa.Check.between(60, 120), nullable=False)
                },strict=True
            )

            try:
                validated_data = schema.validate(df_producao, lazy=True)
                print(f"{len(df_producao)} registros validados com sucesso.")

                return validated_data
            
            except SchemaError as e:
                print(f"Erro de validação:\n{str(e)}")

        except Exception as e:
            print(f"Erro crítico em validate_producao_table: {str(e)}")


    def validate_incidentes_table(self, df_incidentes: Optional[pd.DataFrame]) -> pd.DataFrame:
        """
        Valida os dados de Incidentes gerados usando Pandera e retorna None caso sucesso.
        
        Args:
            df_incidentes (Optional[DataFrame]): DataFrame com os dados de Incidentes para validação.

        Returns:
            DataFrame: Em caso de sucesso, retorna DataFrame validado, senão retorna SchemaError(s).

        Raises:
            ValueError: Se o DataFrame estiver vazio.
            SchemaError: Se validação falhar
        """
        try:
            if df_incidentes is None or df_incidentes.empty:
                raise ValueError("Erro: Não será possível validar os dados, nenhum DataFrame foi passado.")
            else:
                print(f"{len(df_incidentes)} registros para serem validados.")

            schema = pa.DataFrameSchema(
                {
                    "cod_incidente": pa.Column(str, unique=True, nullable=False),
                    "cod_poco": pa.Column(str, nullable=False),
                    "cod_equipamento": pa.Column(str, nullable=False),
                    "data_incidente": pa.Column(date, nullable=False),
                    "tipo_incidente": pa.Column(str, pa.Check.isin(['Falha de Equipamento', 'Parada Programada', 'Vazamento Contido', 'Queda de Pressão', 'Obstrução', 'Manutenção Emergencial']), nullable=False),
                    "severidade": pa.Column(str, pa.Check.isin(['Baixa', 'Média', 'Alta']), nullable=False),
                    "tempo_parada_horas": pa.Column(float, pa.Check.between(1, 168), nullable=False),
                    "custo_estimado_reais": pa.Column(int, pa.Check.between(50_000, 5_000_000), nullable=False),
                    "status_resolucao": pa.Column(str, pa.Check.isin(['Resolvido', 'Em Andamento', 'Pendente']), nullable=False)
                }, strict=True
            )

            try:
                validated_data = schema.validate(df_incidentes, lazy=True)
                print(f"{len(validated_data)} registros validados com sucesso.")

                return validated_data
            
            except SchemaError as e:
                print(f"Erro de validação\n {str(e)}")

        except Exception as e:
            print(f"Erro crítico em validate_incidentes_table: {str(e)}")