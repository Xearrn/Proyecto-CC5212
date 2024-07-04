import pandas as pd

path = "C:/Users/dfaun/OneDrive - ug.uchile.cl/Uch/Semestre 9/Procesamiento masivo de datos/Proyecto"

df = pd.read_csv(path + "/data.csv", on_bad_lines='skip')

reduced_df = df.sample(n=2000000, random_state=1)

reduced_df.to_csv(path + "/final_data.csv", index=False)