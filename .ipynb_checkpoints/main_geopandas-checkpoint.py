import pandas as pd
import geopandas as gpd
from contextlib import contextmanager
import time

folder_path = "./data_EU"

@contextmanager
def get_time(task_name):
    start = time.time()
    yield
    elapsed = time.time() - start
    global total_time
    total_time += elapsed
    print(f"{task_name}... DONE in {(elapsed/60):.2f} min" \
          if elapsed >= 60 else f"{task_name}... DONE in {elapsed:.2f} sec")


def chudu(df):
    rows, cols = df.shape
    print(f"Total rows: {rows}, Total columns: {cols}")
    
    memory = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Memory usage: {memory:.2f} MB")
    
    print("\nFirst 5 rows:")
    print(df.head(5))
    
    unique_fields = [col for col in df.columns if df[col].is_unique]
    print("\nCols with unique values no repeats:")
    print(unique_fields if unique_fields else "None")
    
    nan_columns = df.isna().sum()
    nan_columns = nan_columns[nan_columns > 0]
    print("\nColumns with NaN:")
    print(nan_columns if not nan_columns.empty else "None")
    print()
    print()

def join_EU():
    # grids = gpd.read_file(f"{folder_path}/census_grid_EU/grids.gpkg")  # 4594018 records
    grids = pd.read_pickle("./pickles/grids.pkl")  # 4594018 records
    countries = gpd.read_file(f"{folder_path}/countries_shp/countries.shp")  # 259 records

    grids['GRD_ID'] = grids['GRD_ID'].str.replace("CRS3035RES1000m", "", regex=False)

    grids = grids[['GRD_ID', 'T', 'geometry']]
    countries = countries[['CNTR_ID', 'NAME_ENGL', 'geometry']]

    grids_enr = gpd.sjoin(grids, countries, how="left", predicate="intersects")
    chudu(grids_enr)
    
    # grids.fillna("emle", inplace=True)
    
join_EU()

