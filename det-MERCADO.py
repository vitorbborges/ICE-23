import pandas as pd
import numpy as np
from funcs import *
import unidecode
import basedosdados as bd

# ---------------------------------------------------------------------------------------------
# 2.4. DETERMINANTE MERCADO

mercado = {}

# ---------------------------------------------------------------------------------------------
# 2.4.1. Subdeterminante Desenvolvimento Econômico

subdet = 'Desenvolvimento Econômico'

# 2.4.1.1. Indicador Índice de Desenvolvimento Humano

ind = pd.read_excel('Arquivos ICE - 23/Ind_Originais_ICE_2022.xlsx', header=[i for i in range(6)], index_col=[0,1])
sub_desenveco = ind['Mercado', 'Desenvolvimento Econômico', 'Índice de Desenvolvimento Humano']
sub_desenveco.columns = sub_desenveco.columns.droplevel([0,1])
sub_desenveco.columns.values[0] = 'Índice de Desenvolvimento Humano'
sub_desenveco = sub_desenveco.rename_axis(None, axis=1)
sub_desenveco.index.names = ['Município', 'UF']

# 2.4.1.2. Indicador Crescimento Médio Real do PIB

pib = pd.read_excel('DETERMINANTE MERCADO/tabela5938.xlsx', header=3).head(-1)
pib.columns.values[0] = 'Município'
pib['UF'] = pib['Município'].apply(lambda x: x.split('(')[-1][:2])
pib['Município'] = pib['Município'].apply(lambda x: x.split('(')[0].strip())
pib = pib.set_index(['Município', 'UF'])


deflator = pd.DataFrame([{
    '2016':1.171085,
    '2017':1.120725,
    '2018':1.081036,
    '2019':1
}]).T


pib_def = pd.DataFrame()
for i in pib.columns:
    pib_def[i] = pib[i]*deflator.loc[i,0]
    
pib_var = (pib_def.T / pib_def.T.shift(1)).apply(lambda x: x-1).T.drop('2016', axis=1)
pib_var['Crescimento Real Médio do PIB'] = pib_var.mean(axis=1)

sub_desenveco = pd.merge(sub_desenveco, pib_var['Crescimento Real Médio do PIB'], left_index=True, right_index=True)

# 2.4.1.3. Indicador Número de Empresas Exportadoras com Sede na Cidade

emp_exp = pd.read_excel('DETERMINANTE MERCADO/EMPRESAS_CADASTRO_2020.xlsx', header=7)

convert = lambda x: unidecode.unidecode(x.upper())
n_exp = {n:len(emp_exp.groupby(['MUNICÍPIO', 'UF']).get_group(tuple([convert(i) for i in n]))) for n in sub_desenveco.index}
n_exp = pd.DataFrame([n_exp],index=['n_exp']).T
n_exp.index = pd.MultiIndex.from_tuples(n_exp.index, names=['Município', 'UF'])

variaveis = ('COUNT(quantidade_vinculos_ativos), id_municipio')

## Montando a query
base = '`basedosdados.br_me_rais.microdados_estabelecimentos`'
project_id = "trim-descent-346220"
query = f"SELECT {variaveis} FROM {base} WHERE ano = 2020 AND quantidade_vinculos_ativos > 0 GROUP BY id_municipio"

## Importando o data lake
df_rais = bd.read_sql(query=query, billing_project_id=project_id).set_index('id_municipio')

cod = pd.read_excel('DETERMINANTE MERCADO/RELATORIO_DTB_BRASIL_MUNICIPIO.xls')
cod = cod.rename(columns={'Código Município Completo':'id_municipio'})
cod['id_municipio'] = cod['id_municipio'].apply(str)
cod = cod[['Nome_Município','Nome_UF','id_municipio']].set_index('id_municipio')

n_rais = pd.merge(cod, df_rais, left_index=True, right_index=True).rename(columns={
    'Nome_Município':'Município',
    'Nome_UF':'UF',
    'f0_':'n_rais'
}).set_index(['Município', 'UF'])

ratio = pd.merge(n_rais, n_exp, left_index=True, right_index=True)
ratio['ratio'] = ratio['n_exp']/ratio['n_rais']

sub_desenveco['Número de Empresas Exportadoras com Sede na Cidade'] = ratio['ratio']

missing_data(sub_desenveco)
extreme_values(sub_desenveco)
create_subindex(sub_desenveco, subdet)
mercado[subdet] = sub_desenveco

# ---------------------------------------------------------------------------------------------
# 2.4.2. Subdeterminante Clientes Potenciais

subdet = 'Clientes Potenciais'
sub_clipot = pd.DataFrame()

# 2.4.2.1. Indicador PIB per capita

amostra = pd.read_csv('AMOSTRA/100-municipios.csv').rename(columns={
    'COD. MUNIC':'Município'
}).rename(columns={'Município':'None', 'NOME DO MUNICÍPIO':'Município'}).set_index(['Município', 'UF'])

sub_clipot['PIB per capita'] = (pib['2019']/amostra['POPULAÇÃO ESTIMADA']).dropna()

# 2.4.2.2. Indicador Proporção entre Grandes/Médias e Médias/Pequenas Empresas

variaveis = ('id_municipio, COUNT(quantidade_vinculos_ativos)')
base = '`basedosdados.br_me_rais.microdados_estabelecimentos`'
project_id = "trim-descent-346220"

df_rais = pd.DataFrame()

col = 'Pequenas Empresas'
condition = 'quantidade_vinculos_ativos BETWEEN 10 AND 49'
query = f"SELECT {variaveis} FROM {base} WHERE ano = 2020 AND {condition} GROUP BY id_municipio"
query
df_rais_peq = bd.read_sql(query=query, billing_project_id=project_id).set_index('id_municipio').rename(columns={'f0_':col})

col = 'Médias Empresas'
condition = 'quantidade_vinculos_ativos BETWEEN 50 AND 249'
query = f"SELECT {variaveis} FROM {base} WHERE ano = 2020 AND {condition} GROUP BY id_municipio"
query
df_rais_med = bd.read_sql(query=query, billing_project_id=project_id).set_index('id_municipio').rename(columns={'f0_':col})

col = 'Grandes Empresas'
condition = 'quantidade_vinculos_ativos > 249'
query = f"SELECT {variaveis} FROM {base} WHERE ano = 2020 AND {condition} GROUP BY id_municipio"
query
df_rais_gra = bd.read_sql(query=query, billing_project_id=project_id).set_index('id_municipio').rename(columns={'f0_':col})

df_rais = pd.merge(df_rais_peq, df_rais_med, left_index=True, right_index=True)
df_rais = pd.merge(df_rais, df_rais_gra, left_index=True, right_index=True)
df_rais['Med/Peq'] = df_rais['Médias Empresas']/df_rais['Pequenas Empresas']
df_rais['Gra/Med'] = df_rais['Grandes Empresas']/df_rais['Médias Empresas']
df_rais['ind'] = df_rais['Gra/Med']/df_rais['Med/Peq']

ind_rais = pd.merge(cod, df_rais['ind'], left_index=True, right_index=True).rename(columns={
    'Nome_Município':'Município',
    'Nome_UF':'UF',
    'f0_':'n_rais'
}).set_index(['Município', 'UF'])

sub_clipot['Proporção entre Grandes/Médias e Médias/Pequenas Empresas'] = ind_rais['ind']

# 2.4.2.3. Indicador Compras Públicas

finbra = pd.read_csv('DETERMINANTE MERCADO/finbra.csv', header=3, encoding='latin-1', sep=';')

cond = (finbra['Conta'] == '3.0.00.00.00 - Despesas Correntes') | (finbra['Conta'] == '4.4.00.00.00 - Investimentos')
desp = finbra.loc[np.where(cond)]
desp['Cod.IBGE'] = desp['Cod.IBGE'].apply(str)
desp['Valor'] = desp['Valor'].apply(lambda x: x.replace(',','.')).astype(float)

desp = desp.groupby('Cod.IBGE').agg('sum')
desp = pd.merge(cod, desp, left_index=True, right_index=True).rename(columns={
    'Nome_Município':'Município',
    'Nome_UF':'UF',
    '0':'despesa'
}).set_index(['Município', 'UF']).drop('População', axis=1)

sub_clipot['Compras Públicas'] = desp['Valor']

df = pd.read_csv('DETERMINANTE MERCADO/finbradf.csv', header=3, encoding='latin-1', sep=';')
df = df.iloc[np.where(df['UF'] =='DF')]
cond = (df['Conta'] == '3.0.00.00.00 - Despesas Correntes') | (df['Conta'] == '4.4.00.00.00 - Investimentos')
df = df.iloc[np.where(cond)]
sub_clipot.at['Brasília', 'Compras Públicas'] = sum(df['Valor'].apply(lambda x: x.replace(',','.')).astype(float))

variaveis = ('id_municipio, COUNT(quantidade_vinculos_ativos)')
base = '`basedosdados.br_me_rais.microdados_estabelecimentos`'
project_id = "trim-descent-346220"

df_rais = pd.DataFrame()

col = 'N Empresas'
condition = 'quantidade_vinculos_ativos > 0'
query = f"SELECT {variaveis} FROM {base} WHERE ano = 2020 AND {condition} GROUP BY id_municipio"
query
df_rais = bd.read_sql(query=query, billing_project_id=project_id).set_index('id_municipio').rename(columns={'f0_':col})

ind_rais = pd.merge(cod, df_rais[col], left_index=True, right_index=True).rename(columns={
    'Nome_Município':'Município',
    'Nome_UF':'UF'
}).set_index(['Município', 'UF'])
    
sub_clipot['Compras Públicas'] = sub_clipot['Compras Públicas']/ind_rais[col]

sub_clipot

missing_data(sub_clipot)
extreme_values(sub_clipot)
create_subindex(sub_clipot, subdet)
mercado[subdet] = sub_clipot

# ---------------------------------------------------------------------------------------------

mercado = pd.concat(mercado, axis=1)
create_detindex(mercado, 'Mercado')

mercado.to_csv('DETERMINANTES/det-MERCADO.csv')