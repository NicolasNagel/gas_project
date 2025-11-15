import pandas as pd
import numpy as np
import random

from datetime import datetime, timedelta
from typing import Optional, List, Dict
from faker import Faker

fake = Faker('pt_BR')

class FakeData():
    """Classe para criar as tabelas de exemplo do projeto usando Faker."""
    def __init__(self):
        pass


    def generate_pocos_table(self, tamanho_lote: int = 100, lista_dados: Optional[List] = None) -> pd.DataFrame:
        """
        Gera dados de cadastro de Poços usando Faker e retorna um Dataframe.

        Args:
            - tamanho_lote (int): Quantidade de Dados a serem gerados, por padrão gera 100 registros.
            - lista_dados (Optional[List]): Lista de cadastro de Poços já salvos no Banco de Dados
                usado para não duplicar os cadastros a cada nova geração.

        Returns:
            Dataframe: DataFrame com os dados estruturados para validação com o Pandera.
        """
        try:
            if not lista_dados:
                print("Sem registros para serem verificados no Banco.")
                lista_dados = []
            
            chunk_size = 50
            chunks = []
            pocos_cadastrados = set(lista_dados)
            novos_pocos = []
            
            for chunk_start in range(0, tamanho_lote, chunk_size):
                chunk_end = min(chunk_start + chunk_size, tamanho_lote)
                chunk_data = []

                for _ in range(chunk_start, chunk_end):
                    try:
                        while True:
                            cod_poco = f"POCO_{random.randint(100, 6606)}"
                            if cod_poco not in pocos_cadastrados or cod_poco not in novos_pocos:
                                novos_pocos.append(cod_poco)
                                break

                        lista_pocos = [1, 2] # 1 = Marítimo | 2 = Terrestre
                        tipo_poco = random.choices(lista_pocos, weights=[0.8, 0.2])[0]
                        if tipo_poco == 1:
                            profundidade = random.randint(2_000, 7_000)
                            localizacao = random.choice(['Bacia de santos', 'Bacia de Campos', 'Bacia do Espírito Santos'])
                        else:
                            profundidade = random.randint(500, 3_000)
                            localizacao = random.choice(['Bacia do Recôncavo', 'Bacia Potiguar'])

                        camada = random.choices(['Pre-Sal', 'Pos-Sal'], weights=[0.78, 0.22])[0]
                        status = random.choices(['Ativo', 'Manutenção', 'Inativo'], weights=[0.85, 0.10, 0.05])[0]
                        operadora = random.choices(
                            ['Petrobras', 'Shell', 'TotalEnergies', 'Equinor'],
                            weights=[0.90, 0.05, 0.03, 0.02]
                        )[0]

                        chunk_data.append({
                            "codigo_poco": cod_poco,
                            "nome_poco": f"{fake.city()}-{random.randint(1, 100)}",
                            "tipo_poco": tipo_poco,
                            "localizacao": localizacao,
                            "camada": camada,
                            "profundidade_metros": profundidade,
                            "status_operacional": status,
                            "data_perfuracao": fake.date_between(start_date='-10y', end_date='-1y'),
                            "operadora": operadora
                        })

                    except Exception as e:
                        print(f"Erro ao gerar o poço: {e}")
                        continue

                if not chunk_data:
                    continue

                try:
                    df_chunk = pd.DataFrame(chunk_data)
                    chunks.append(df_chunk)

                    print(f"    Gerados {chunk_end}/{tamanho_lote} cadastros...")
                except Exception as e:
                    print(f"    Erro ao criar o DataFrame do chunk: {str(e)}")
                    continue

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                print(f"    Total de {len(df)} pocos gerados com sucesso.")
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            print(f"Erro critico em generate_pocos_table: {str(e)}")
            return pd.DataFrame()
        
    def generate_equipamentos_table(
            self, 
            tamanho_lote: int = 500,
            df_pocos: Optional[pd.DataFrame] = None,
            lista_equipamentos: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """
        Gera dados de cadastro de Equipamentos usando Faker e retorna um Dataframe.

        Args:
            - tamanho_lote (int): Quantidade de Dados a serem gerados, por padrão gera 500 registros.
            - df_pocos (Optional[DataFrame]): Lista de cadastro de Poços já salvos no Banco de Dados
                usado para não duplicar os cadastros a cada nova geração.
            - lista_equipamentos (Optional[List[str]]): Lista de código de equipamentos já cadastrados.

        Returns:
            Dataframe: DataFrame com os dados estruturados para validação com o Pandera.
        """
        try:
            if df_pocos is None or df_pocos.empty:
                print("Erro: Não é possível gerar equipamentos, nenhum poço foi passado.")
                return pd.DataFrame()
            
            if lista_equipamentos is None:
                lista_equipamentos = []
                print(f"Sem equipamento cadastrados no Banco de Dados.")

            chunk_size = 100
            chunks = []
            equipamentos_cadastrados = set(lista_equipamentos)
            novos_equipamentos = []

            nome_pocos = df_pocos[['codigo_poco', 'data_perfuracao']].to_dict('records')

            for chunk_start in range(0, tamanho_lote, chunk_size):
                chunk_end = min(chunk_start + chunk_size, tamanho_lote)
                chunk_data = []

                for _ in range(chunk_start, chunk_end):
                    try:
                        while True:
                            cod_equipamento = f"EQUIP_{random.randint(100, 9_999)}"
                            if cod_equipamento not in equipamentos_cadastrados and cod_equipamento not in novos_equipamentos:
                                novos_equipamentos.append(cod_equipamento)
                                break

                        poco_selecionado = random.choice(nome_pocos)
                        cod_poco = poco_selecionado['codigo_poco']
                        data_perfuracao_poco = poco_selecionado['data_perfuracao']

                        equipamento = random.choice(
                            ['Bomba Submersível', 'FPSO', 'Válvula DHSV', 'Sistema de Elevação', 'Compressor', 'Separador']
                        )
                        marca = random.choice(
                            ['Schulemberger', 'Haliburton', 'Baker Hughes', 'Weatherford', 'NOV']
                        )
                        modelo = f"{marca}-{equipamento}-{random.randint(100, 9_999)}"

                        intervalo = random.randint(30, 75)
                        data_instalacao = data_perfuracao_poco + timedelta(days=intervalo)
                        vida_util = random.randint(10, 25)

                        ultimo_teste = fake.date_between(start_date="-6m", end_date="today")
                        eficiencia = random.randint(60, 100)

                        chunk_data.append({
                            "cod_equipamento": cod_equipamento,
                            "cod_poco": cod_poco,
                            "tipo_equipamento": equipamento,
                            "marca": marca,
                            "modelo": modelo,
                            "data_instalacao": data_instalacao,
                            "vida_util_anos": vida_util,
                            "ultimo_teste": ultimo_teste,
                            "eficiencia_percentual": eficiencia
                        }) 

                    except Exception as e:
                        print(f"Erro ao gerar equipamento: {str(e)}")
                        return pd.DataFrame()
                    
                if not chunk_data:
                    continue

                try:
                    df_chunk = pd.DataFrame(chunk_data)
                    chunks.append(df_chunk)

                    print(f"    Gerados {chunk_end}/{tamanho_lote} cadastros...")
                except Exception as e:
                    print(f"    Erro ao criar o DataFrame no chunk: {str(e)}")

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                print(f"    Total de {len(df)} equipamentos gerados com suceso.")
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Erro crítico em generate_equipamentos_table: {str(e)}")
            return pd.DataFrame()
        

    def generate_producao_table(
            self,
            tamanho_lote: int = 500,
            df_pocos: Optional[pd.DataFrame] = None,
            lista_producao: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """
        Gera dados de produção aleatórios usando Faker e retorna um DataFrame.

        Args:
            - tamanho_lote (int): Quantidade de Dados a serem gerados, por padrão gera 500 registros.
            - df_pocos (Optinonal[DataFrame]): Lista de cadastro de poços gerados, usado para referenciar
                os dados na hora da geração.
            - lista_producao (Optional[List[str]]): Lista de cadastro dos registros de produção, usada para
                não duplicar os registros existentes.

        Returns:
            DataFrame: DataFrame com os dados estruturados para validação com Pandera.
        """
        try:
            if df_pocos is None or df_pocos.empty:
                print("Erro: Não é possível gerar registros de produção, nenhum poço foi passado.")
                return pd.DataFrame()
            
            if lista_producao is None:
                lista_producao = []
                print("Sem registros de Produção no Banco de Dados.")

            chunk_size = 100
            chunks = []
            registros_producao = set(lista_producao)
            novos_registros_producao = []

            data_pocos = df_pocos[['codigo_poco', 'tipo_poco', 'data_perfuracao']].to_dict('records')

            for chunk_start in range(0, tamanho_lote, chunk_size):
                chunk_end = min(chunk_start + chunk_size, tamanho_lote)
                chunk_data = []

                for _ in range(chunk_start, chunk_end):
                    try:
                        while True:
                            cod_producao = f"PROD-{random.randint(1, 9_999)}"
                            if cod_producao not in registros_producao and cod_producao not in novos_registros_producao:
                                registros_producao.add(cod_producao)
                                break

                        id_poco = random.choice(data_pocos)
                        nome_poco = id_poco['codigo_poco']
                        data_perfuracao = id_poco['data_perfuracao']
                        tipo_poco = id_poco['tipo_poco']

                        intervalo = random.randint(45, 90)
                        data_producao = fake.date_between(
                            start_date=(data_perfuracao + timedelta(days=intervalo)),
                            end_date='today'
                        )

                        if tipo_poco == 1:
                            producao_barris_dia = random.randint(50_000, 200_000)
                        else:
                            producao_barris_dia = random.randint(100, 5_000)

                        agua_produzida = (producao_barris_dia * random.randint(10, 40))
                        tempo_operacao_horas = random.uniform(0.0, 24.0)
                        pressao_bar = random.randint(150, 450)
                        temperatura_celsius = random.uniform(60.0, 120.0)

                        chunk_data.append({
                            "cod_producao": cod_producao,
                            "cod_poco": nome_poco,
                            "data_producao": data_producao,
                            "petroleo_barris_dia": producao_barris_dia,
                            "agua_produzida_m3": agua_produzida,
                            "tempo_horas_operacao": tempo_operacao_horas,
                            "pressao_bar": pressao_bar,
                            "temperatura_celsius": temperatura_celsius
                        })

                    except Exception as e:
                        print(f"Erro ao gerar registro de produção: {str(e)}")
                        return pd.DataFrame()

                if not chunk_data:
                    continue

                try:
                    df_chunk = pd.DataFrame(chunk_data)
                    chunks.append(df_chunk)
                    print(f"    Gerados {chunk_end}/{tamanho_lote} registros...")
                except Exception as e:
                    print(f"    Erro ao criar o DataFrame no chunk: {str(e)}")

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                print(f"    Total de {len(df)} equipamentos gerados com suceso.")
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            print(f"Erro crítico em generate_producao_table: {str(e)}")
            return pd.DataFrame()
        
    def generate_incidentes_table(
            self,
            tamanho_lote: int = 250,
            df_equipamentos: Optional[pd.DataFrame] = None,
            df_producao: Optional[pd.DataFrame] = None,
            lista_incidentes: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """
        Gera dados de incidentes usando Fake e retorna um DataFrame.

        Args:
            tamanho_lote (int): Quantidade de Dados a serem gerados, por padrão gera 250 registros.
            df_equipamentos (Optional[DataFrame]): DataFrame com os registros de equipamentos e poços
                usado para referenciar os dados na hora da geração.
            df_producao (Optional[DataFrame]): DataFrame com os registros de produção usado para
                referenciar os dados de *data* na hora da geração
            lista_incidentes (Optional[List[str]]): Lista de incidentes já gerados, usada para garantir
                que nenhum registro de incidente seja duplicado.
        
        Returns:
            DataFrame: DataFrame com os dados estruturados para validação com Pandera.
        """
        try:
            if df_equipamentos is None or df_equipamentos.empty:
                print("Erro: Não foi possível gerar registros de incidentes. Nenhum equipamento foi passado.")
                return pd.DataFrame()
            elif df_producao is None or df_producao.empty:
                print("Erro: Não foi possível gerar registros de incidentes. Nenhum registro de produção foi passado.")
                return pd.DataFrame()
            
            if lista_incidentes is None:
                lista_incidentes = []
                print(f"Sem registros de incidentes no Banco de Dados.")

            chunk_size = 125
            chunks = []
            incidentes_cadastrados = set(lista_incidentes)
            novos_incidentes = []

            data_equipamentos = df_equipamentos[['cod_equipamento', 'cod_poco']].to_dict('records')
            data_producao = df_producao[['cod_poco', 'data_producao']].to_dict('records')

            for chunk_start in range(0, tamanho_lote, chunk_size):
                chunk_end = min(chunk_start + chunk_size, tamanho_lote)
                chunk_data = []

                for _ in range(chunk_start, chunk_end):
                    try:
                        while True:
                            cod_incidente = f"INC-{random.randint(1, 9_999)}"
                            if cod_incidente not in novos_incidentes and cod_incidente not in incidentes_cadastrados:
                                incidentes_cadastrados.add(cod_incidente)
                                break

                        id_equipamento = random.choice(data_equipamentos)
                        cod_equipamento = id_equipamento['cod_equipamento']
                        cod_poco = id_equipamento['cod_poco']

                        producoes_poco = [
                            prod for prod in data_producao if prod['cod_poco'] == cod_poco
                        ]
                        if not producoes_poco:
                            continue

                        datas_producao = [prod['data_producao'] for prod in producoes_poco]
                        dias_producao_min = min(datas_producao)
                        data_incidente = fake.date_between(start_date=dias_producao_min, end_date='today')
                        
                        tipo_incidente = [
                            'Falha de Equipamento', 'Parada Programada', 'Vazamento Contido',
                            'Queda de Pressão', 'Obstrução', 'Manutenção Emergencial'
                            ]
                        
                        severidade = random.choices(['Baixa', 'Média', 'Alta'], weights=[0.5, 0.35, 0.15])[0]
                        tempo_parada_horas = random.uniform(1.0, 168.0)
                        custo_estimado_reais = random.randint(50_000, 5_000_000)
                        status_resolucao = random.choices(['Resolvido', 'Em Andamento', 'Pendente'], weights=[0.7, 0.2, 0.1])[0]

                        chunk_data.append({
                            "cod_incidente": cod_incidente,
                            "cod_poco": cod_poco,
                            "cod_equipamento": cod_equipamento,
                            "data_incidente": data_incidente,
                            "tipo_incidente": random.choice(tipo_incidente),
                            "severidade": severidade,
                            "tempo_parada_horas": tempo_parada_horas,
                            "custo_estimado_reais": custo_estimado_reais,
                            "status_resolucao": status_resolucao
                        })

                    except Exception as e:
                        print(f"Erro ao gerar registros de incidentes: {str(e)}")
                        return pd.DataFrame()
                
                if not chunk_data:
                    continue

                try:
                    df_chunk = pd.DataFrame(chunk_data)
                    chunks.append(df_chunk)
                    print(f"    Gerados {chunk_end}/{tamanho_lote} registros gerados.")
                except Exception as e:
                    print(f"    Erro ao criar o DataFrame no chunk: {str(e)}")

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                print(f"    Gerados {len(df)} registros com sucesso.")
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Erro crítico em generate_incidentes_table: {str(e)}")
            return pd.DataFrame()