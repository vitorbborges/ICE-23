import pandas as pd
import numpy as np
from funcs import *

# ---------------------------------------------------------------------------------------------
# 2.3. DETERMINANTE INFRAESTRUTURA

infraestrutura = {}

# ---------------------------------------------------------------------------------------------
# 2.3.1. Subdeterminante Transporte Interurbano

subdet = 'Transporte Interurbano'

# ---------------------------------------------------------------------------------------------
# 2.3.1.1 Indicador Conectividade via Rodovias

ind = pd.read_excel('Arquivos ICE - 23/Ind_Originais_ICE_2022.xlsx', header=[i for i in range(6)], index_col=[0,1])
sub_transurb = ind['Infraestrutura', 'Transporte Interurbano', 'Conectividade Via Rodovias']
sub_transurb.columns = sub_transurb.columns.droplevel([0,1])
sub_transurb.columns.values[0] = 'Conectividade Via Rodovias'
sub_transurb = sub_transurb.rename_axis(None, axis=1)
sub_transurb.index.names = ['Município', 'UF']

# 2.3.1.2. Indicador Número de Decolagens por Ano

deco = pd.read_csv('DETERMINANTE INFRAESTRUTURA/voos_brasil.csv').drop('sigla_aero',axis=1).rename(columns={
    'sigla_uf':'sigla_uf_ref',
    'nome':'nome_ref'
})

ref = pd.read_csv('https://raw.githubusercontent.com/manginidouglas/ice2021/main/infraestrutura/aeroportos/sd21_voos_completo.xlsx')
ref['sigla_uf_ref'] = ref['sigla_uf_ref'].fillna(ref['sigla_uf'])
ref['nome_ref'] = ref['nome_ref'].fillna(ref['nome'])
ref = ref[['nome', 'sigla_uf','nome_ref','sigla_uf_ref']].rename(columns={
    'sigla_uf':'UF',
    'nome':'Município'
})

deco = pd.merge(deco,ref, how='inner', on=['nome_ref', 'sigla_uf_ref'])
deco = deco.groupby(['Município','UF'], as_index=False).agg('sum').set_index(['Município', 'UF'])

#deco.to_csv('DETERMINANTE INFRAESTRUTURA/decolagens.csv')  # rodar somente na primera vez depois comentar
# salvando o indicador para mudar manualmente casos de letra maiuscula

deco = pd.read_csv('DETERMINANTE INFRAESTRUTURA/decolagens.csv').set_index(['Município', 'UF']).rename(columns={
    'decolagens': 'Número de Decolagens por Ano'
})
sub_transurb = pd.merge(sub_transurb, deco, left_index=True, right_index=True)

# 2.3.1.3. Indicador Distância ao Porto Mais Próximo

portos = pd.read_csv('DETERMINANTE INFRAESTRUTURA/sd22_portos.csv').rename(columns={
    'i213':'Distância ao Porto Mais Próximo',
    'nome':'Município',
    'sigla_uf':'UF'
}).drop('id_municipio',axis=1).set_index(['Município', 'UF'])
sub_transurb = pd.merge(sub_transurb, portos, left_index=True, right_index=True)

missing_data(sub_transurb)
extreme_values(sub_transurb)
create_subindex(sub_transurb, subdet)
infraestrutura[subdet] = sub_transurb

# Voltando as colunas negativas ao normal para salvar os dados:

sub_transurb['Distância ao Porto Mais Próximo'] = negative(sub_transurb['Distância ao Porto Mais Próximo'])

# ---------------------------------------------------------------------------------------------
# 2.3.2. Subdeterminante Condições Urbanas

subdet = 'Condições Urbanas'

# 2.3.2.1. Indicador Acesso à Internet Rápida

banda = pd.read_csv('DETERMINANTE INFRAESTRUTURA/Acessos_Banda_Larga_Fixa_2021.csv', sep=';')
banda = banda.groupby(['Município', 'UF']).agg('sum')['Acessos']
pop = pd.read_csv('AMOSTRA/100-municipios.csv').rename(columns={'NOME DO MUNICÍPIO':'Município'}).set_index(['Município', 'UF'])

ind_int = pd.DataFrame()
ind_int['Acesso à Internet Rápida'] = (banda/pop['POPULAÇÃO ESTIMADA']).dropna()
sub_condurb = ind_int

# 2.3.2.2. Indicador Preço Médio do m²

cod = pd.read_excel('DETERMINANTE MERCADO/RELATORIO_DTB_BRASIL_MUNICIPIO.xls').drop(['Município','UF'],axis=1)
cod = cod.rename(columns={
    'Código Município Completo':'id_municipio',
    'Nome_Município':'Município',
    'Nome_UF':'UF'
})
cod['id_municipio'] = cod['id_municipio'].apply(str)
cod = cod[['Município','UF','id_municipio']].set_index('id_municipio')

ind_m2 = pd.read_csv('DETERMINANTE INFRAESTRUTURA/sd22_m2_completo.csv')[['id_municipio', 'm2']]
ind_m2['id_municipio'] = ind_m2['id_municipio'].apply(str)
ind_m2 = ind_m2.set_index('id_municipio')

ind_m2 = pd.merge(cod, ind_m2, left_index=True, right_index=True).reset_index(drop=True).set_index(['Município', 'UF'])
sub_condurb['Preço Médio do m²'] = negative(ind_m2['m2'])

# 2.3.2.3. Indicador Custo da Energia Elétrica

# COLETA REALIZADA EM 6 DE SETEMBRO

distri = pd.read_csv('DETERMINANTE INFRAESTRUTURA/distribuidoras.csv').drop('UF', axis=1)
distri['Distribuidora'] = distri['Distribuidora'].apply(lambda x: x.upper() if type(x) != type(1.5) else None)
ranking = pd.read_excel('DETERMINANTE INFRAESTRUTURA/RankingB1.xlsx')[['Distribuidora', 'UF', 'Tarifa Convencional']]
ranking['Distribuidora'] = ranking['Distribuidora'].apply(lambda x: x.upper() if type(x) != type(1.5) else None)

atual = pd.merge(distri,ranking, on='Distribuidora').drop('Distribuidora', axis=1)

# adicionar manualmente a media ponderada do preço nas cidades com mais de uma distribuidora
atual = atual.append({
    'Município':'Campina Grande',
    'UF':'PB',
    'Tarifa Convencional':0.568994603
}, ignore_index=True)
atual = atual.append({
    'Município':'Duque de Caxias',
    'UF':'RJ',
    'Tarifa Convencional':0.817278973
}, ignore_index=True)
atual = atual.append({
    'Município':'Petrópolis',
    'UF':'RJ',
    'Tarifa Convencional':0.826959994
}, ignore_index=True)
atual = atual.append({
    'Município':'Santa Maria',
    'UF':'RS',
    'Tarifa Convencional':0.644020688
}, ignore_index=True)
atual = atual.append({
    'Município':'Guarujá',
    'UF':'SP',
    'Tarifa Convencional':0.622065737
}, ignore_index=True)
atual = atual.append({
    'Município':'Mogi das Cruzes',
    'UF':'SP',
    'Tarifa Convencional':0.637558772
}, ignore_index=True)
atual = atual.append({
    'Município':'Praia Grande',
    'UF':'SP',
    'Tarifa Convencional':0.620542118
}, ignore_index=True)
atual = atual.append({
    'Município':'Santos',
    'UF':'SP',
    'Tarifa Convencional':0.620119901
}, ignore_index=True)
atual = atual.append({
    'Município':'São José do Rio Preto',
    'UF':'SP',
    'Tarifa Convencional':0.685554926
}, ignore_index=True)
atual = atual.append({
    'Município':'São Paulo',
    'UF':'SP',
    'Tarifa Convencional':0.594588207
}, ignore_index=True)
atual = atual.append({
    'Município':'Sorocaba',
    'UF':'SP',
    'Tarifa Convencional':0.620400308
}, ignore_index=True)
atual = atual.append({
    'Município':'Suzano',
    'UF':'SP',
    'Tarifa Convencional':0.637468135
}, ignore_index=True)
atual = negative(atual.rename(columns={'Tarifa Convencional':'Custo da Energia Elétrica'}).set_index(['Município', 'UF']))
sub_condurb = pd.merge(sub_condurb, atual, left_index=True, right_index=True)

# 2.3.2.4. Indicador Taxa de Homicídios

deaths = pd.read_csv('DETERMINANTE INFRAESTRUTURA/A194712189_28_143_208.csv', encoding='latin-1',sep=';', header=4).head(-8)
deaths['Município'] = deaths['Município'].apply(lambda x: x.split()[0])
deaths = deaths.set_index('Município')

a = pd.merge(cod.reset_index().set_index(['Município', 'UF']), pop, left_index=True, right_index=True).reset_index()
a['id_municipio'] = a['id_municipio'].apply(lambda x:x[:-1])
a = a.set_index('id_municipio')[['Município', 'UF', 'POPULAÇÃO ESTIMADA']]

deaths = pd.merge(a, deaths, left_index=True, right_index=True).reset_index(drop=True).set_index(['Município', 'UF'])
ind_deaths = pd.DataFrame()
ind_deaths['Taxa de Homicídios'] = negative(deaths['Óbitos_p/Ocorrênc']*100000/deaths['POPULAÇÃO ESTIMADA'])

sub_condurb = pd.merge(sub_condurb, ind_deaths, left_index=True, right_index=True)

missing_data(sub_condurb)
extreme_values(sub_condurb)
create_subindex(sub_condurb, subdet)
infraestrutura[subdet] = sub_condurb

# Voltando as colunas negativas ao normal para salvar os dados:

sub_condurb['Preço Médio do m²'] = negative(sub_condurb['Preço Médio do m²'])
sub_condurb['Custo da Energia Elétrica'] = negative(sub_condurb['Custo da Energia Elétrica'])
sub_condurb['Taxa de Homicídios'] = negative(sub_condurb['Taxa de Homicídios'])

# ---------------------------------------------------------------------------------------------

infraestrutura = pd.concat(infraestrutura, axis=1)
create_detindex(infraestrutura, 'Infraestrutura')

infraestrutura.to_csv('DETERMINANTES/det-INFRAESTRUTURA.csv')