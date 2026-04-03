def run(slowly_changing_customers: int):

    import random
    import pandas as pd
    import numpy as np

    from modules.randomizer import Randomizer
    from modules.const import SCD2_DATA_CSV_PATH, CSV_DIR

    df_collection = Randomizer.dataframe_collection(CSV_DIR)
    df = pd.concat(df_collection, axis=0, ignore_index=True)

    if slowly_changing_customers > len(df['Customer ID'].unique()):
        raise RuntimeError(f"Il numero di clienti in cambiamento non può essere maggiore di {len(df)} (numero clienti univoci).")

    # `slowly_changing_customers` è il numero di Customers che avranno modifica al campo Segment
    rand_customer_ids = np.random.choice(df['Customer ID'].unique(), size=slowly_changing_customers)
    segments = set(df['Segment'])
    df = df[df['Customer ID'].isin(rand_customer_ids)].copy()
    for _, row in df.iterrows():
        order_id = row['Order ID']
        actual_segment = row['Segment']
        rand_segments = segments - {actual_segment}
        df.loc[df['Order ID'] == order_id, 'Segment'] = random.choice(list(rand_segments))

    df.to_csv(SCD2_DATA_CSV_PATH, encoding='utf-8', index=False)
    print()
