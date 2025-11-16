from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class PocosTable(Base):
    __tablename__ = 'raw_pocos'

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True, nullable=False)
    codigo_poco = Column(String, unique=True, nullable=False)
    nome_poco = Column(String, nullable=False)
    tipo_poco = Column(String, nullable=False)
    localizacao = Column(String, nullable=False)
    camada = Column(String, nullable=False)
    profundidade_metros = Column(Integer, nullable=False)
    status_operacional = Column(String, nullable=False)
    data_perfuracao = Column(DateTime, nullable=False)
    operadora = Column(String, nullable=False)
    data_insercao = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    equipamentos = relationship("EquipamentosTable", back_populates="pocos")
    producao = relationship("ProducaoTable", back_populates="pocos")
    incidentes = relationship("IncidentesTable", back_populates="pocos")

class EquipamentosTable(Base):
    __tablename__ = 'raw_equipamentos'

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True, nullable=False)
    cod_equipamento = Column(String, unique=True, nullable=False)
    cod_poco = Column(String, ForeignKey('raw_pocos.codigo_poco'), nullable=False)
    tipo_equipamento = Column(String, nullable=False)
    marca = Column(String, nullable=False)
    modelo = Column(String, nullable=False)
    data_instalcao = Column(DateTime, nullable=False)
    vida_util_anos = Column(Integer, nullable=False)
    ultimo_teste = Column(DateTime, nullable=False)
    eficiencia_operacional = Column(Float, nullable=False)
    data_insercao = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    pocos = relationship("PocosTable", back_populates="equipamentos")
    incidentes = relationship("IncidentesTable", back_populates="equipamentos")

class ProducaoTable(Base):
    __tablename__ = 'raw_producao'

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True, nullable=False)
    cod_producao = Column(String, unique=True, nullable=False)
    cod_poco = Column(String, ForeignKey('raw_pocos.codigo_poco'), nullable=False)
    data_producao = Column(DateTime, nullable=False)
    petroleo_barris_dia = Column(Integer, nullable=False)
    agua_produzida_m3 = Column(Float, nullable=False)
    tempo_horas_operacao = Column(Float, nullable=False)
    pressao_bar = Column(Integer, nullable=False)
    temperatura_celsius = Column(Float, nullable=False)
    data_insercao = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    pocos = relationship("PocosTable", back_populates='producao')

class IncidentesTable(Base):
    __tablename__ = 'raw_incidentes'

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True, nullable=False)
    cod_incidente = Column(String, unique=True, nullable=False)
    cod_poco = Column(String, ForeignKey('raw_pocos.codigo_poco'), nullable=False)
    cod_equipamento = Column(String, ForeignKey('raw_equipamentos.cod_equipamento'), nullable=False)
    data_incidente = Column(DateTime, nullable=False)
    tipo_incidente = Column(String, nullable=False)
    severidade = Column(String, nullable=False)
    tempo_parada_horas = Column(Float, nullable=False)
    custo_estimado_reais = Column(Integer, nullable=False)
    status_resolucao = Column(String, nullable=False)
    data_insercao = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    pocos = relationship("PocosTable", back_populates='incidentes')
    equipamentos = relationship("EquipamentosTable", back_populates='incidentes')