import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os
import time

start = time.time()

load_dotenv(find_dotenv())

subfolder = os.getenv("SUBFOLDER")

files = Path(subfolder).glob("**/*.json")

dataframes = [pd.read_json(file) for file in files]
combined_df = pd.concat(dataframes, ignore_index=True)
table = pa.Table.from_pandas(combined_df)

pq.write_table(table, "out/python.parquet")

end = time.time()
print(end - start)