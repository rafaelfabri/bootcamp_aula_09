import pandas as pd
import os 
import glob



def extrair_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, "*.json"))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)

    #print(df_list)
    #print(df_total)
    
    return df_total

def calcualr_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    
    df['Calculo'] = df['Quantidade'] * df['Venda']

    return df

def carregar_dados(df: pd.DataFrame, formato_saida: str):
    
    if formato_saida == 'csv':
        
        df.to_csv('dados.csv', index = False)
    
    if formato_saida == 'parquet':
        
        df.to_parquet('dados.parquet')


def pipeline(pasta: str, formato_saida: str):
    
    print(extrair_dados(pasta))
    
    df = extrair_dados(pasta)
    
    df_calculo = calcualr_kpi_de_total_de_vendas(df.copy())
    
    print(df_calculo)
    
    carregar_dados(df_calculo, formato_saida)

    
