# SQL Exercise

Pipeline quasiETL che costruisce uno **Star Schema** in **SQLite3** su un dataset di vendite di un e-commerce, dopodiché lo migra su **DuckDB**.

## Cosa fa

1. Genera CSV annuali sintetici basandosi su `train.csv` e usando `Randomizer`:
2. Genera `train_SCD.csv` per testare logiche SCD Type 2 su `DimCustomers`;
3. Crea e popola lo star schema in SQLite (`superstore.db`);
4. Avvia un *bulk insert* per inserire nel DB i file CSV generati.
5. Applica SCD Type 2 su `DimCustomers`
6. Migra lo schema intero in **DuckDB** (`superstore.duckdb`)

## Schema

Staging layer: `StrictCopy` (raw import, date come VARCHAR) -> `Master` (tipizzato, date ISO).

Dimensioni: `DimProducts`, `DimCustomers` (SCD Type 2), `DimGeography`.

Fatti: `FactSales` (referenziato su chiavi esterne dalle 3 dimensioni).

## Setup

```bash
pip install -r requirements.txt
```

Posizionare il CSV file sorgente in `csv/train.csv`.

## Utilizzo

```bash
python main.py  # defaults: 5 years, 300 slowly-changing customers
python main.py -y 3 -s 100           # 3 synthetic years, 100 customers with Segment changes
python main.py --years 3 --scd2 100  # 3 synthetic years, 100 customers with Segment changes
```

| Argument | Description | Default |
| --- | --- | --- |
| `-y, --years` | Numero di anni sintetici da generare | `5` |
| `-s, --scd2` | Customers che cambiano Segment (SCD2 test) | `300` |

Se `superstore.db` esiste già, la fase *bootstrap* viene saltata e la pipeline comincia dall'UPSERT.

## Queries

Nella cartella `queries` sono presenti 5 analisi business eseguite su DuckDB:

| File | Analisi |
| --- | --- |
| `01 - yoy_growth.sql` | Revenue YoY per categoria con variazione percentuale |
| `02 - cohort.sql` | Cohort retention: revenue per anno di prima acquisizione |
| `03 - abc_analysis.sql` | Classificazione ABC dei prodotti per revenue cumulato |
| `04 - shipping.sql` | Shipping performance e anomalie per ShipMode |
| `05 - geo.sql` | Penetrazione geografica con rank regionale e segmentazione |

Per eseguirle tutte in sequenza in modo indipendente dalla pipeline principale (`main.py`):

```bash
python queries/_run.py
```

Lo script legge i file `.sql` in ordine alfabetico, stampa un'anteprima del risultato e attende conferma prima di procedere al successivo.

In alternativa, `queries/notebook.py` è un notebook interattivo che esegue le stesse query su DuckDB e ne mostra i risultati navigabili nel browser:

```bash
marimo run queries/notebook.py
```

In assenza della libreria `marimo`, si può leggere output simile da `queries/notebook.html`.

Per avere la traduzione in Python (**Pandas**) del flusso del notebook SQL:

```bash
jupyter notebook queries/pandas.ipynb
```

**NOTA**
Gli output di *notebook.py*, *notebook.html* e *pandas.ipynb* sono gli stessi.

## Struttura

```plain
.
├── main.py             # Main orchestrations
├── modules/
│   ├── const.py        # Costanti e percorsi
│   ├── func.py         # DB utilities
│   ├── randomizer.py   # Generazione dati random
│   └── types.py        # Column, ForeignKey, Index dataclasses
├── source/
│   ├── tbls_init.py
│   ├── index_init.py
│   ├── factsales_init.py
│   ├── bulk_update_from_csv.py
│   ├── random_yearly_data.py
│   ├── random_scd2_data.py
│   └── migration.py
├── csv/
│   ├── train.csv          # Source data
│   └── train_SCD.csv      # Generato a runtime
└── queries/            # Simulazioni Business Analysis
    ├── __init__.py
    ├── _run.py         # unico runner: legge .sql, esegue, esporta CSV
    ├── 01 - yoy_growth.sql
    ├── 02 - cohort.sql
    ├── 03 - abc_analysis.sql
    ├── 04 - shipping.sql
    ├── 05 - geo.sql
    ├── notebook.py     # SQL notebook (richiede marimo)
    ├── notebook.html   # Stampa del SQL notebook
    └── pandas.ipynb    # Pandas notebook
```
