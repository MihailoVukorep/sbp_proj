import pandas as pd
df = pd.read_csv("dataset/TMDB_movie_dataset_v11.csv")

print(df.dtypes)

print(df.shape)

for i in df.columns:
    print(i, df[i].nunique())
