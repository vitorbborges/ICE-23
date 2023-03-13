"""
Created on Thu Aug 18 16:50:42 2022

@author: CLIENTE
"""

import pandas as pd
from funcs import missing_data,extreme_values,create_subindex,negative,create_detindex
#from functools import reduce
import basedosdados as bd

# 1. AMOSTRA
database = pd.DataFrame()

amostra = pd.read_csv('AMOSTRA/100-municipios.csv', converters={i: str for i in range(0,101)})
amostra['Cod.IBGE'] = amostra['COD. UF'] + amostra['COD. MUNIC']
database['Município'] = amostra['NOME DO MUNICÍPIO'].str.upper().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
database['UF'] = amostra['UF']
database['Cod.IBGE'] = amostra['Cod.IBGE']
database = database.set_index(['Município', 'UF'])

# 2.7. DETERMINANTE CAPITAL HUMANO
capital_humano = {}

## 2.7.1. Subdeterminante Acesso e Qualidade da Mão de Obra Básica
subdet = 'Acesso e Qualidade da Mão de Obra Básica'

### 2.7.1.1. Indicador nota ideb
df_ideb = pd.read_excel('Arquivos ICE - 23/Ind_Originais_ICE_2022.xlsx', header=5,
                        usecols="B:C,BA")
subdet_acesso = df_ideb.rename(columns={df_ideb.columns[1]:'Município',
                                        df_ideb.columns[2]:'Nota do IDEB'})

subdet_acesso = subdet_acesso.merge(amostra, how='right', left_on='Município',
                                    right_on='NOME DO MUNICÍPIO')
subdet_acesso = subdet_acesso[['UF_x','Município','Nota do IDEB','Cod.IBGE']]
subdet_acesso = subdet_acesso.rename(columns={'UF_x':'UF'})

### 2.7.1.2. Indicador proporção de adultos com pelo menos o ensino médio completo
df_enem = pd.read_csv('DETERMINANTE CAPITAL HUMANO/ENEM_2021_100mun.csv')
 
alvo = ['E','F','M']

pai_EM,mae_EM,num_inscritos = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()

num_inscritos['n_inscritos'] = df_enem.groupby('CO_MUNICIPIO_ESC').size()
pai_EM['pai_EM'] = df_enem[df_enem['Q001'].isin(alvo)].groupby('CO_MUNICIPIO_ESC').size()
mae_EM['mae_EM'] = df_enem[df_enem['Q002'].isin(alvo)].groupby('CO_MUNICIPIO_ESC').size()

pai_mae_EM = pai_EM.merge(mae_EM, how='inner', on='CO_MUNICIPIO_ESC')
pai_mae_EM = pai_mae_EM.merge(num_inscritos, how='inner', on='CO_MUNICIPIO_ESC')

pai_mae_EM['prop_pai_EM'] = pai_mae_EM['pai_EM']/pai_mae_EM['n_inscritos']
pai_mae_EM['prop_mae_EM'] = pai_mae_EM['mae_EM']/pai_mae_EM['n_inscritos']
pai_mae_EM['Proporção de Adultos com pelo menos o Ensino Médio Completo'] = (pai_mae_EM['prop_pai_EM']+pai_mae_EM['prop_mae_EM'])/2

interesse = ['Proporção de Adultos com pelo menos o Ensino Médio Completo']
pai_mae_EM = pai_mae_EM[interesse].reset_index()
pai_mae_EM['CO_MUNICIPIO_ESC'] = pai_mae_EM['CO_MUNICIPIO_ESC'].astype(str)

subdet_acesso = subdet_acesso.merge(pai_mae_EM, how='right', left_on='Cod.IBGE',
                                    right_on='CO_MUNICIPIO_ESC')

### 2.7.1.3. Indicador Taxa Líquida de Matrícula no Ensino Médio
#### Pessoas entre 15 e 17 anos no município (população 2010)
variaveis = 'id_setor_censitario, sigla_uf, v049, v050, v051'
base = '`basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010`'
project_id = 'double-balm-306418'
query = (f'SELECT {variaveis} FROM {base}')

df_censo_15_17 = bd.read_sql(query=query, billing_project_id=project_id)
df_censo_15_17['Cod.IBGE'] = df_censo_15_17['id_setor_censitario'].str[:7]
df_censo_15_17['UF'] = df_censo_15_17['sigla_uf'].str[:2]
df_censo_15_17 = df_censo_15_17.merge(database, how='right', on='Cod.IBGE')
df_censo_15_17 = df_censo_15_17.dropna()

df_censo_15_17 = df_censo_15_17.iloc[:,2:7].set_index(['Cod.IBGE','UF'])
df_censo_15_17.iloc[:,0:3] = df_censo_15_17.iloc[:,0:3].apply(pd.to_numeric)
df_censo_15_17 = df_censo_15_17.groupby('Cod.IBGE').sum()
df_censo_15_17['pop_15_17'] = df_censo_15_17.sum(axis=1)

#### População 2010
variaveis = 'id_municipio, populacao'
base = '`basedosdados.br_ibge_populacao.municipio`'
project_id = 'double-balm-306418'
cod_ibge = tuple(database['Cod.IBGE'].astype(str))
query = (f'SELECT {variaveis} FROM {base} WHERE ano = 2010 AND id_municipio IN {cod_ibge}')

pop_2010 = bd.read_sql(query=query, billing_project_id=project_id)
populacao = pop_2010.merge(amostra, left_on='id_municipio', right_on='Cod.IBGE')
interesse = ['Cod.IBGE','populacao','POPULAÇÃO ESTIMADA']
populacao = populacao[interesse]
populacao['tx_crecimento'] = 1 + (populacao['POPULAÇÃO ESTIMADA'].astype(int)-populacao['populacao'])/populacao['populacao']

df_censo_15_17 = df_censo_15_17.merge(populacao, how='right', on='Cod.IBGE')
df_censo_15_17['pop_15_17_atualizado'] = df_censo_15_17['pop_15_17']*df_censo_15_17['tx_crecimento']

interesse = ['Cod.IBGE','pop_15_17_atualizado']
df_censo_15_17 = df_censo_15_17[interesse]

#### censo escolar população entre 15 e 17 anos
df_ce_2021 = pd.read_csv('DETERMINANTE CAPITAL HUMANO/CE_2021_100mun.csv',
                         sep=',', encoding='latin-1')
df_ce_2021 = df_ce_2021[['CO_MUNICIPIO','QT_MAT_MED']].dropna()
df_ce_2021 = df_ce_2021.groupby('CO_MUNICIPIO').sum().reset_index()
df_ce_2021['CO_MUNICIPIO'] = df_ce_2021['CO_MUNICIPIO'].astype(str)

df_ce_2021 = df_ce_2021.merge(df_censo_15_17, left_on='CO_MUNICIPIO',
                              right_on='Cod.IBGE')

df_ce_2021['Taxa Líquida de Matrícula no Ensino Médio'] = df_ce_2021['QT_MAT_MED']/df_ce_2021['pop_15_17_atualizado']

interesse = ['Cod.IBGE','Taxa Líquida de Matrícula no Ensino Médio']
df_ce_2021 = df_ce_2021[interesse]

subdet_acesso = subdet_acesso.merge(df_ce_2021, how='right', on='Cod.IBGE')

### 2.7.1.4. Indicador Nota Média no Enem
nota_enem = df_enem[['CO_MUNICIPIO_ESC','NU_NOTA_CH','NU_NOTA_CN',
                     'NU_NOTA_LC','NU_NOTA_MT','NU_NOTA_REDACAO']].dropna()
nota_enem = nota_enem.groupby('CO_MUNICIPIO_ESC').mean()
nota_enem['Nota Média no ENEM'] = nota_enem.mean(axis=1)
nota_enem = nota_enem['Nota Média no ENEM'].reset_index()
nota_enem['CO_MUNICIPIO_ESC'] = nota_enem['CO_MUNICIPIO_ESC'].astype(str)

subdet_acesso = subdet_acesso.merge(nota_enem, left_on='Cod.IBGE', 
                                    right_on='CO_MUNICIPIO_ESC')

### 2.7.1.5. Indicador Proporção de Matriculados no Ensino Técnico e Profissionalizante
#### População maior que 15 anos 
base = '`basedosdados.br_ibge_censo_demografico.setor_censitario_idade_total_2010`'
project_id = 'double-balm-306418'
query = (f'SELECT * FROM {base}')

df_censo_15 = bd.read_sql(query=query, billing_project_id=project_id)
df_censo_15['Cod.IBGE'] = df_censo_15['id_setor_censitario'].str[:7]
df_censo_15['UF'] = df_censo_15['sigla_uf'].str[:2]
df_censo_15 = df_censo_15.set_index(['UF','Cod.IBGE'])
df_censo_15 = df_censo_15.iloc[:,50:137].reset_index()
df_censo_15 = df_censo_15.merge(database, how='right', on='Cod.IBGE').dropna()
df_censo_15.iloc[:,2:89] = df_censo_15.iloc[:,2:89].apply(pd.to_numeric)
df_censo_15 = df_censo_15.groupby('Cod.IBGE').sum().reset_index()
df_censo_15['pop_maior_15'] = df_censo_15.sum(axis=1)

interesse=['Cod.IBGE','pop_maior_15']
df_censo_15 = df_censo_15[interesse].merge(populacao, how='right', on='Cod.IBGE')
df_censo_15['atualizada_pop_maior_15'] = df_censo_15['pop_maior_15']*df_censo_15['tx_crecimento']
interesse=['Cod.IBGE','atualizada_pop_maior_15']
df_censo_15=df_censo_15[interesse]

#### Censo escolar
df_ce_tec = pd.read_csv('DETERMINANTE CAPITAL HUMANO/CE_2021_100mun.csv',
                         sep=',', encoding='latin-1')
df_ce_tec = df_ce_tec[['CO_MUNICIPIO','QT_MAT_PROF_TEC']].dropna()
df_ce_tec = df_ce_tec.groupby('CO_MUNICIPIO').sum().reset_index()
df_ce_tec['CO_MUNICIPIO'] = df_ce_tec['CO_MUNICIPIO'].astype(str)

df_ce_tec = df_ce_tec.merge(df_censo_15, left_on='CO_MUNICIPIO',right_on='Cod.IBGE')

df_ce_tec['Proporção de Matriculados no Ensino Técnico e Profissionalizante'] = df_ce_tec['QT_MAT_PROF_TEC']/df_ce_tec['atualizada_pop_maior_15']
interesse = ['Cod.IBGE','Proporção de Matriculados no Ensino Técnico e Profissionalizante']
df_ce_tec = df_ce_tec[interesse]

subdet_acesso = subdet_acesso.merge(df_ce_tec, how='right', on='Cod.IBGE')
subdet_acesso = subdet_acesso.set_index(['Município','UF'])
del subdet_acesso['Cod.IBGE']
del subdet_acesso['CO_MUNICIPIO_ESC_x']
del subdet_acesso['CO_MUNICIPIO_ESC_y']

missing_data(subdet_acesso)
extreme_values(subdet_acesso)
create_subindex(subdet_acesso, subdet)
capital_humano[subdet] = subdet_acesso

## 2.7.2. Subdeterminante Acesso e Qualidade da Mão de Obra Qualificada
subdet = 'Acesso e Qualidade da Mão de Obra Qualificada'
### 2.7.2.1. Indicador Proporção de Adultos com Pelo Menos o Ensino Superior Completo
alvo = ['F','G']
pai_SUP,mae_SUP = pd.DataFrame(),pd.DataFrame()

pai_SUP['pai_SUP'] = df_enem[df_enem['Q001'].isin(alvo)].groupby('CO_MUNICIPIO_ESC').size()
mae_SUP['mae_SUP'] = df_enem[df_enem['Q002'].isin(alvo)].groupby('CO_MUNICIPIO_ESC').size()

pai_mae_SUP = pai_SUP.merge(mae_SUP, how='inner', on='CO_MUNICIPIO_ESC')
subdet_acesso_quali = pai_mae_SUP.merge(num_inscritos, how='inner', on='CO_MUNICIPIO_ESC')

subdet_acesso_quali['prop_pai_SUP'] = subdet_acesso_quali['pai_SUP']/subdet_acesso_quali['n_inscritos']
subdet_acesso_quali['prop_mae_SUP'] = subdet_acesso_quali['mae_SUP']/subdet_acesso_quali['n_inscritos']
subdet_acesso_quali['Proporção de Adultos com pelo menos os Ensino Superior Completo'] = (subdet_acesso_quali['prop_pai_SUP']+subdet_acesso_quali['prop_mae_SUP'])/2
subdet_acesso_quali = subdet_acesso_quali['Proporção de Adultos com pelo menos os Ensino Superior Completo'].reset_index()

### 2.7.2.2. Indicador Proporção de Alunos Concluintes em Cursos de Alta Qualidade
df_enade = pd.read_excel('Arquivos ICE - 23/Ind_Originais_ICE_2022.xlsx', header=5,
                        usecols="B:C,BH")
df_enade = df_enade.rename(columns={
    df_enade.columns[1]:'Município',
    df_enade.columns[2]:'Proporção de Alunos Concluintes em Cursos de Alta Qualidade'})
df_enade = df_enade.merge(amostra, how='right', left_on='Município',right_on='NOME DO MUNICÍPIO')
df_enade = df_enade[['UF_x','Município','Proporção de Alunos Concluintes em Cursos de Alta Qualidade','Cod.IBGE']]
df_enade = df_enade.rename(columns={'UF_x':'UF'})

subdet_acesso_quali['CO_MUNICIPIO_ESC'] = subdet_acesso_quali['CO_MUNICIPIO_ESC'].astype(str)
subdet_acesso_quali = subdet_acesso_quali.merge(df_enade, right_on='Cod.IBGE',
                                                left_on='CO_MUNICIPIO_ESC')
order = ['Cod.IBGE','Município','UF','Proporção de Adultos com pelo menos os Ensino Superior Completo','Proporção de Alunos Concluintes em Cursos de Alta Qualidade']
subdet_acesso_quali = subdet_acesso_quali[order]

### Indicador Custo Médio de Salários de Dirigentes
cbo_2002 = tuple(['121005','121010','122105','122110','122115','122120','122205',
                  '122305','122405','122505','122510','122515','122520','122605',
                  '122610','122615','122620','122705','122710','122715','122720',
                  '122725','122730','122735','122740','122745','122750','122755',
                  '123105','123110','123115','123205','123210','123305','123310',
                  '123405','123410','123605','123705','123805','131105','131110',
                  '131115','131120','131205','131210','131215','131220','131225',
                  '131305','131310','131315','131320','141105','141110','141115',
                  '141120','141205','141305','141405','141410','141415','141420',
                  '141505','141510','141515','141520','141525','141605','141610',
                  '141615','141705','141710','141715','141720','141725','141730',
                  '141735','141805','141810','141815','141820','141825','141830',
                  '142105','142110','142115','142120','142125','142130','142205',
                  '142210','142305','142310','142315','142320','142325','142330',
                  '142335','142340','142345','142350','142405','142410','142415',
                  '142505','142510','142515','142520','142525','142530','142535',
                  '142605','142610','142705','142710'])

variaveis = 'valor_remuneracao_media,id_municipio'
base = '`basedosdados.br_me_rais.microdados_vinculos`'
project_id = 'double-balm-306418'
cod_ibge = tuple(database['Cod.IBGE'].astype(str))
query = (f'SELECT {variaveis} FROM {base} WHERE ano = 2019 AND cbo_2002 IN {cbo_2002}'
         f' AND id_municipio IN {cod_ibge}')

df_rais = bd.read_sql(query=query, billing_project_id=project_id)
df_rais = df_rais.groupby('id_municipio').agg(['count','sum'])
df_rais['Custo Médio de Salários de Dirigentes'] = df_rais.iloc[:,1]/df_rais.iloc[:,0] 
interesse = ['Custo Médio de Salários de Dirigentes']
df_rais = df_rais[interesse].reset_index().droplevel(level=1, axis=1)
df_rais['Custo Médio de Salários de Dirigentes'] = negative(df_rais['Custo Médio de Salários de Dirigentes'])

subdet_acesso_quali = subdet_acesso_quali.merge(df_rais, right_on='id_municipio',
                                                left_on='Cod.IBGE')
subdet_acesso_quali = subdet_acesso_quali.set_index(['Município','UF'])
del subdet_acesso_quali['Cod.IBGE']
del subdet_acesso_quali['id_municipio']

missing_data(subdet_acesso_quali)
extreme_values(subdet_acesso_quali)
create_subindex(subdet_acesso_quali, subdet)
capital_humano[subdet] = subdet_acesso_quali

subdet_acesso_quali['Custo Médio de Salários de Dirigentes'] = negative(subdet_acesso_quali['Custo Médio de Salários de Dirigentes'])

# -
capital_humano = pd.concat(capital_humano, axis=1)
create_detindex(capital_humano, 'Capital Humano')

capital_humano.to_csv('DETERMINANTES/det-CAPITAL HUMANO.csv')








