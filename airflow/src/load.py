import pandas as pd
import utils

def load_postgres(df:pd.DataFrame, table_name:str, engine_str:str) -> None:
    engine = utils.create_engine(engine_str)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

def save_parquet(df:pd.DataFrame, output:str) -> None:
    df.to_parquet(output, index=False)
    
def save_csv(df:pd.DataFrame, output:str) -> None:
    df.to_csv(output + ".csv", index=False,sep='|')