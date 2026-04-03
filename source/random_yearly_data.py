def run(years: int) -> None:

    import random

    from modules.randomizer import Randomizer
    from modules.const import CSV_DIR

    for y in range(years):

        rows_per_year = random.randint(1000, 2000)

        rand = Randomizer(CSV_DIR, rows_per_year)
        print(f"\n[{y+1}/{years}] Generando dati anno {rand._year}")

        rand.create_new_csv_with_data()

        print(f"Inserite {rows_per_year:,} righe.")
