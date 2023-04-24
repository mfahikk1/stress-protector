import pandas as pd
import os
import multiprocessing

COMBINED_DATA_PATH = "C:\\Users\\MAC\\Downloads\\fahik\\processed_data1"
SAVE_PATH = "C:\\Users\\MAC\\Downloads\\fahik\\Combined_Data"

# Define signals
signals = ['acc', 'eda', 'hr', 'temp']

# if COMBINED_DATA_PATH != SAVE_PATH:
#    os.mkdir(SAVE_PATH)

print("Reading data ...")

def read_parallel(signal):
    filepath = os.path.join(COMBINED_DATA_PATH, f"combined_{signal}.csv")
    print(f"Reading file: {filepath}")
    df = pd.read_csv(filepath, dtype={'id': str})
    print(f"Finished reading file: {filepath}")
    return [signal, df]

if __name__ == '__main__':
    pool = multiprocessing.Pool(len(signals))
    results = pool.map(read_parallel, signals)
    pool.close()
    pool.join()

    for i in results:
        globals()[i[0]] = i[1]

    # Merge data
    print('Merging Data ...')
    ids = eda['id'].unique()
    columns = ['X', 'Y', 'Z', 'EDA', 'HR', 'TEMP', 'id', 'datetime']

def merge_parallel(id, columns, acc, eda, hr, temp):
    print(f"Processing {id}")
    df = pd.DataFrame(columns=columns)

    if acc is not None:
        acc_id = acc[acc['id'] == id]
        if not acc_id.empty:
            df = df.merge(acc_id, on='datetime', how='outer')

    if eda is not None:
        eda_id = eda[eda['id'] == id].drop(['id'], axis=1)
        if not eda_id.empty:
            df = df.merge(eda_id, on='datetime', how='outer')

    if hr is not None:
        hr_id = hr[hr['id'] == id].drop(['id'], axis=1)
        if not hr_id.empty:
            df = df.merge(hr_id, on='datetime', how='outer')

    if temp is not None:
        temp_id = temp[temp['id'] == id].drop(['id'], axis=1)
        if not temp_id.empty:
            df = df.merge(temp_id, on='datetime', how='outer')

    df.fillna(method='ffill', inplace=True)
    df.fillna(method='bfill', inplace=True)

    return df

if __name__ == '__main__':
    pool = multiprocessing.Pool(len(ids))
    results = pool.starmap(merge_parallel, [(id, columns, acc, eda, hr, temp) for id in ids])
    pool.close()
    pool.join()

    new_df = pd.concat(results, ignore_index=True)

    print("Saving data ...")
    new_df.to_csv(os.path.join(SAVE_PATH, "merged_data.csv"), index=False)
