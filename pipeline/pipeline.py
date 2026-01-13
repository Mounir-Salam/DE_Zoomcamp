import sys
import pandas as pd

# Output of sys.argv look like this: ['pipeline.py', 'input_1', 'input_2', ...]
print("arguments", sys.argv)

day = int(sys.argv[1])

print(f"pipeline run for day: {day}")


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['day'] = day

print(df.head())

df.to_parquet(f"output_{day}.parquet")