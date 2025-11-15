import pandas as pd
import pandera.pandas as pa

from abc import ABC
from typing import Optional
from datetime import date
from pandera.errors import SchemaError

from src.data.generate_fake_data import FakeData

class ValidateSchema(ABC):
    """Classe de validção de Schema usando Pandera."""
    def __init__(self):
        pass

    def validate_pocos_table(self, df_pocos: Optional[pd.DataFrame]) -> None:
        """Valida os dados de Poços Gerados usando Pandera e retorna None caso sucesso.
        
        Args:
            df_pocos (Optional[DataFrame]): DataFrame com os dados de poços gerados para validação.

        Returns:
            None: Em caso de sucesso, retorna **None**, senão retorna SchemaError(s).
        """
        try:
            if df_pocos is None or df_pocos.empty:
                return SchemaError("Erro: Não será possível validar os dados, nenhum DataFrame foi passado.")
            else:
                print(f"{len(df_pocos)} registros para serem validados.")

            schema = pa.DataFrameSchema({
                "codigo_poco": pa.Column(str, nullable=False),
                "nome_poco": pa.Column(str, nullable=False),
                "tipo_poco": pa.Column(int, pa.Check.ge(0), nullable=False),
                "localizacao": pa.Column(str, nullable=False),
                "camada": pa.Column(str, nullable=False),
                "profundidade_metros": pa.Column(int, pa.Check.ge(0), nullable=False),
                "status_operacional": pa.Column(str, nullable=False),
                "data_perfuracao": pa.Column(date, nullable=False),
                "operadora": pa.Column(str, nullable=False) 
            })
            validated_data = schema.validate(df_pocos)
            print(f"{len(validated_data)} registros validados com sucesso...")

            return validated_data 
        
        except Exception as e:
            print(f"Erro crítico em validate_pocos_table: {str(e)}")


if __name__ == '__main__':

    fake_data = FakeData()
    validate_data = ValidateSchema()

    df_pocos = fake_data.generate_pocos_table()
    validate_df_pocos = validate_data.validate_pocos_table(df_pocos)
    print(validate_df_pocos)
