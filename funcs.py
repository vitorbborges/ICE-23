import pandas as pd
import numpy as np

# 3.1. Tratamento para Indicadores com Impacto Negativo no Empreendedorismo

def negative(series):
    
    series = 1/series
    
    return series.replace(np.inf, 0)

# 3.2. Tratamento para Observações Faltantes (missing data)

def missing_data(df):
    
    ind = pd.read_excel('Arquivos ICE - 23/Ind_Originais_ICE_2022.xlsx')
    ind.columns = ind.iloc[1]
    ind.columns.values[0] = 'Município'
    ind = ind.set_index('Município')
    ind = ind.tail(101)
    
    for c in df.columns:
        if c in ind.columns:
            if df[c].isna().sum()/len(df) > 0.3:
                df[c].fillna(ind[c], inplace=True)
        
        df[c] = df[c].fillna(0)
                
    return df

# 3.3. Tratamento para Valores Extremos

def extreme_values(df):
    
    for c in df.columns:
    
        df_sort = np.argsort(df[c])        
            
        top_values = df_sort[-3:]
        bottom_values = df_sort[:3]
        removed =[]
        
        if top_values[-1] > 5*np.mean(top_values[:-1]):
            removed.append(top_values[-1])
        
        if bottom_values[0] > 5*np.mean(bottom_values[1:]):
            removed.append(bottom_values[0])

        for r in df.index:
            if df.loc[r,c] in removed:
                df.at[r,c] = 0
            
    return df

# 3.4. Padronização de Indicadores

def normalize(series):
    return (series - series.mean())/series.std()

def create_subindex(df, subdet):
    
    i_name = 'Índice de '+subdet
    
    if i_name not in df.columns:
        norm_data = df.apply(lambda x: normalize(x), axis=0)
        df[i_name] = normalize(norm_data.sum(axis=1)) + 6
    
    return df

def create_detindex(df, det):
    
    d_name = 'Índice de ' + det
    det_df = pd.DataFrame()
    
    if d_name not in df.columns:
        for i in (df.columns.levels[0]):
            det_df[i] = df[i,(df[i].columns[-1])]

        det_df = det_df.apply(lambda x: normalize(x), axis=0)
        det_df[d_name] = normalize(det_df.sum(axis=1)) + 6
        df[d_name] = det_df[d_name]
    
    return df