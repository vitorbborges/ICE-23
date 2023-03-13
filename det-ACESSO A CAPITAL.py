"""
Created on Thu Aug 18 16:50:42 2022

@author: CLIENTE
"""

import pandas as pd
import numpy as np
from funcs import *
#from functools import reduce
import basedosdados as bd

# 1. AMOSTRA
database = pd.DataFrame()

amostra = pd.read_csv('AMOSTRA/100-municipios.csv', converters={i: str for i in range(0,101)})
amostra['Cod.IBGE'] = amostra['COD. UF'] + amostra['COD. MUNIC']
database['Município'] = amostra['NOME DO MUNICÍPIO'].str.upper().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
database['UF'] = amostra['UF']
database['Cod.IBGE'] = amostra['Cod.IBGE']
database['pop_est'] = amostra['POPULAÇÃO ESTIMADA']
database = database.set_index(['Município', 'UF'])

# 2.5.
acesso_capital = {}

## 
subdet = 'Capital Disponível'

### 2.5.1. e 2.5.3.

base = '`basedosdados.br_ibge_pib.municipio`'
project_id = 'double-balm-306418'
var = ('id_municipio, pib')
cod_ibge = tuple(database['Cod.IBGE'].astype(str))
query = f'SELECT {var} FROM {base} WHERE ano = 2019 AND id_municipio IN {cod_ibge}'
pib_mun = bd.read_sql(query=query,billing_project_id=project_id)
pib_mun = pib_mun.rename(columns={'id_municipio':'Cod.IBGE'})

df_bcb = pd.read_excel('DETERMINANTE ACESSO A CAPITAL/202112_ESTBAN.xlsx', 
                       header=2, usecols="B,D,V,AQ,AT")
df_bcb = df_bcb.rename(columns={'MUNICIPIO':'Município'})
df_bcb = database.merge(df_bcb, how='left',on=['Município','UF'])
df_bcb = df_bcb.groupby(by=['Município','UF','Cod.IBGE','pop_est']).sum()
df_bcb = df_bcb.reset_index().merge(pib_mun, how='left', on='Cod.IBGE').set_index(['Município','UF'])
df_bcb['Operações de Crédito por Município'] = df_bcb['VERBETE_160_OPERACOES_DE_CREDITO']/df_bcb['pib']

df_bcb['420+432'] = df_bcb['VERBETE_420_DEPOSITOS_DE_POUPANCA'] + df_bcb['VERBETE_432_DEPOSITOS_A_PRAZO']
df_bcb['Capital Poupado per capita'] = df_bcb['420+432']/df_bcb['pop_est'].astype(np.int64)

subdet_capital_disp = df_bcb.iloc[:,[0,6,8]]
subdet_capital_disp = subdet_capital_disp.merge(amostra, how='right', on='Cod.IBGE').rename(columns={'NOME DO MUNICÍPIO':'Município'})
interesse=['Município','UF','Cod.IBGE','Operações de Crédito por Município',
           'Capital Poupado per capita']
subdet_capital_disp = subdet_capital_disp[interesse]

## 2.5.2. 
df_crunchbase = pd.read_excel('DETERMINANTE ACESSO A CAPITAL/crunchbase_2021.xlsx', usecols="A:C").fillna(0)
df_crunchbase['Município'] = df_crunchbase['Município'].str.upper().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
df_crunchbase = database.merge(df_crunchbase, how='left', on=['Município','UF']).merge(pib_mun, how='left',on='Cod.IBGE')
df_crunchbase['Proporção Relativa de Capital de Risco'] = (df_crunchbase['Total funding amount']*(5.3950)/df_crunchbase['pib'])

subdet_capital_disp = subdet_capital_disp.merge(df_crunchbase, how='right', on='Cod.IBGE')
interesse=['Município_x','UF_x','Operações de Crédito por Município',
           'Proporção Relativa de Capital de Risco','Capital Poupado per capita']
subdet_capital_disp = subdet_capital_disp[interesse]
subdet_capital_disp = subdet_capital_disp.rename(columns={'Município_x':'Município',
                                                          'UF_x':'UF'})
subdet_capital_disp = subdet_capital_disp.set_index(['Município','UF'])

missing_data(subdet_capital_disp)
extreme_values(subdet_capital_disp)
create_subindex(subdet_capital_disp, subdet)
acesso_capital[subdet] = subdet_capital_disp

# -
acesso_capital = pd.concat(acesso_capital, axis=1)
create_detindex(acesso_capital, 'Acesso a Capital')

acesso_capital.to_csv('DETERMINANTES/det-ACESSO A CAPITAL.csv')
