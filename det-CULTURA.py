#!pip install pytrends
from pytrends.request import TrendReq
import json
import pandas as pd
import requests
from pytrends.request import TrendReq
import numpy as np
from funcs import *

INTEREST_BY_REGION_URL = 'https://trends.google.com/trends/api/widgetdata/comparedgeo'


def interest_by_city(self, inc_low_vol=True):
    """Request data from Google's Interest by City section and return a dataframe"""

    # make the request
    resolution = 'CITY'
    region_payload = dict()
    self.interest_by_region_widget['request'][
        'resolution'] = resolution

    self.interest_by_region_widget['request'][
        'includeLowSearchVolumeGeos'] = inc_low_vol

    # convert to string as requests will mangle
    region_payload['req'] = json.dumps(
        self.interest_by_region_widget['request'])
    region_payload['token'] = self.interest_by_region_widget['token']
    region_payload['tz'] = self.tz

    # parse returned json
    req_json = self._get_data(
        url=TrendReq.INTEREST_BY_REGION_URL,
        method='get',
        trim_chars=5,
        params=region_payload,
    )
    df = pd.DataFrame(req_json['default']['geoMapData'])
    if (df.empty):
        return df

    # rename the column with the search keyword
    df = df[['geoName', 'coordinates', 'value', 'hasData']].set_index(
        ['geoName']).sort_index()
    # split list columns into seperate ones, remove brackets and split on comma
    result_df = df['value'].apply(lambda x: pd.Series(
        str(x).replace('[', '').replace(']', '').split(',')))

    # rename each column with its search term
    for idx, kw in enumerate(self.kw_list):
        result_df[kw] = result_df[idx].astype('int')
        del result_df[idx]

    return result_df

# ---------------------------------------------------------------------------------------------
# 1. AMOSTRA

database = pd.DataFrame()
amostra = pd.read_csv('AMOSTRA/100-municipios.csv')
database['Município'] = amostra['NOME DO MUNICÍPIO']
database['UF'] = amostra['UF']
database = database.set_index(['Município', 'UF'])

# ---------------------------------------------------------------------------------------------
# 2.8. DETERMINANTE CULTURA

cultura = {}

ufs = { 'AC': 'State of Acre',
        'AL': 'State of Alagoas',
        'AP': 'State of Amapá',
        'AM': 'State of Amazonas',
        'BA': 'State of Bahia',
        'CE': 'State of Ceará',
        'DF':'Federal District',
        'ES': 'State of Espírito Santo',
        'GO': 'State of Goiás',
        'MA': 'State of Maranhão',
        'MS': 'State of Mato Grosso do Sul',
        'MT': 'State of Mato Grosso',
        'MG': 'State of Minas Gerais',
        'PA': 'State of Pará',
        'PB': 'State of Paraíba',
        'PR': 'State of Paraná',
        'PE': 'State of Pernambuco',
        'PI': 'State of Piauí',
        'RJ': 'State of Rio de Janeiro',
        'RN': 'State of Rio Grande do Norte',
        'RS': 'State of Rio Grande do Sul',
        'RO': 'State of Rondônia',
        'RR': 'State of Roraima',
        'SC': 'State of Santa Catarina',
        'SP': 'State of São Paulo',
        'SE': 'State of Sergipe',
        'TO': 'State of Tocantins'}

# ---------------------------------------------------------------------------------------------
# 2.8.1. SUBDETERMINANTE INICIATIVA

subdet = 'Iniciativa'

iniciativa = ["empreendedora", "empreendedorismo", "mei"]

sub_iniciativa = pd.DataFrame(database)

for term in iniciativa:
    
    pytrends = TrendReq()
    pytrends.build_payload([term], timeframe='today 5-y', geo='BR')
    region = pytrends.interest_by_region(resolution='REGION', inc_low_vol=True, inc_geo_code=False)
    
    region_df = pd.DataFrame()
    
    for uf in ufs.keys():
        
        pytrends = TrendReq()
        pytrends.build_payload([term], timeframe='today 5-y', geo='BR-'+uf)
        city = interest_by_city(pytrends)
        city = city.reset_index().rename(columns={'geoName':'Município'})
        city['UF'] = [uf]*len(city)
        city = city.set_index(['Município', 'UF'])
        city[term] = city[term]* (region.loc[ufs[uf], term]/100)
        
        region_df = pd.concat([region_df, city])
        
    for i in [i for i in database.index if i not in list(region_df.index)]:
        region_df.loc[i,term] = np.nan
    sub_iniciativa = pd.merge(sub_iniciativa, region_df, left_index=True, right_index=True)
    
sub_iniciativa.columns = ['Pesquisas pelo Termo Empreendedora',
                          'Pesquisas pelo Termo Empreendedorismo',
                          'Pesquisas pelo Termo MEI']

missing_data(sub_iniciativa)
extreme_values(sub_iniciativa)
create_subindex(sub_iniciativa, subdet)
cultura[subdet] = sub_iniciativa

# ---------------------------------------------------------------------------------------------
# 2.8.2. SUBDETERMINANTE INSTITUIÇÕES

subdet = 'Instituições'

instituicoes = ["sebrae", "franquia", "simples nacional", "senac"]

sub_instituicoes = pd.DataFrame(database)

for term in instituicoes:
    
    pytrends = TrendReq()
    pytrends.build_payload([term], timeframe='today 5-y', geo='BR')
    region = pytrends.interest_by_region(resolution='REGION', inc_low_vol=True, inc_geo_code=False)
    
    region_df = pd.DataFrame()
    
    for uf in ufs.keys():
        
        pytrends = TrendReq()
        pytrends.build_payload([term], timeframe='today 5-y', geo='BR-'+uf)
        city = interest_by_city(pytrends)
        city = city.reset_index().rename(columns={'geoName':'Município'})
        city['UF'] = [uf]*len(city)
        city = city.set_index(['Município', 'UF'])
        city[term] = city[term]* (region.loc[ufs[uf], term]/100)
        
        region_df = pd.concat([region_df, city])
        
    for i in [i for i in database.index if i not in list(region_df.index)]:
        region_df.loc[i,term] = np.nan
    sub_instituicoes = pd.merge(sub_instituicoes, region_df, left_index=True, right_index=True)
    
sub_instituicoes.columns = ['Pesquisas por Sebrae',
                            'Pesquisas por Franquia',
                            'Pesquisas por SIMPLES Nacional',
                            'Pesquisas por Senac']

missing_data(sub_instituicoes)
extreme_values(sub_instituicoes)
create_subindex(sub_instituicoes, subdet)
cultura[subdet] = sub_instituicoes

# ---------------------------------------------------------------------------------------------
cultura = pd.concat(cultura, axis=1)
cultura
create_detindex(cultura, 'Cultura')

cultura.to_csv('DETERMINANTES/det-CULTURA.csv')