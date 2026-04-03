import csv
from datetime import date, timedelta
from pathlib import Path
import random
import pandas as pd
from typing import Generator, Any

from .const import TRAIN_CSV_PATH

class Randomizer:
    csv_starter = TRAIN_CSV_PATH
    date_format = "%d/%m/%Y"

    def __init__(self, input_dir: Path, new_lines: int) -> None:
        import pandas as pd
        assert input_dir.is_dir()
        self.dst_dir = input_dir
        self.new_lines = new_lines

        self.dataframe = pd.concat(
            Randomizer.dataframe_collection(self.dst_dir),
            axis=0,
            ignore_index=True
        ).sort_values("Row ID").reset_index(drop=True)
        self._year: int = self.max_year + 1
        self._last_row = int(self.dataframe.iloc[-1]['Row ID'])

        self._existing_order_id: list = self.dataframe["Order ID"].dropna().unique().tolist()
        self._last_order_progressive = self._compute_last_orderid_progressive()

    def __len__(self) -> int:
        return len(self.dataframe)

    @property
    def pick_rand_row(self) -> pd.Series:
        return self.dataframe.iloc[random.randrange(len(self))]

    def generate_order(self) -> list[dict[str, Any]]:
        try:
            rand_row = self.pick_rand_row
            customer_id, customer_name, segment = rand_row.loc[
                ["Customer ID", "Customer Name", "Segment"]
            ]
            country, city, state, postal_code, region = rand_row.loc[
                ["Country", "City", "State", "Postal Code", "Region"]
            ]
        except IndexError as e:
            print(e)
            return []

        order_id = self.new_order_id()
        order_date = self.rand_order_date()
        ship_mode = self.rand_ship_mode()
        delay = self.rand_ship_delay(ship_mode)
        ship_date = order_date + delay

        # Numero di linee dell'Order
        n_lines = random.randint(1, 5)

        rows = []
        used_products = set()
        while len(rows) < n_lines:
            product_row = self.pick_rand_row
            product_id = product_row["Product ID"]

            # Questo set assicuro che nello stesso Order
            # non si ripetano alcun Product
            if product_id in used_products:
                continue
            used_products.add(product_id)
            # ------------------------------

            category, sub_category, product_name, sales = product_row.loc[
                ["Category", "Sub-Category", "Product Name", "Sales"]
            ]

            rows.append({
                "Row ID": self.row_id(),
                "Order ID": order_id,
                "Order Date": order_date.strftime(self.date_format),
                "Ship Date": ship_date.strftime(self.date_format),
                "Ship Mode": ship_mode,
                "Customer ID": customer_id,
                "Customer Name": customer_name,
                "Segment": segment,
                "Country": country,
                "City": city,
                "State": state,
                "Postal Code": postal_code,
                "Region": region,
                "Product ID": product_id,
                "Category": category,
                "Sub-Category": sub_category,
                "Product Name": product_name,
                "Sales": sales,
            })

        return rows

    @staticmethod
    def dataframe_collection(directory: Path) -> Generator[pd.DataFrame]:
        """Concatena tutti i csv in unico dataframe"""
        import pandas as pd
        for file in directory.glob('*.csv'):
            df = pd.read_csv(file, encoding='utf-8', dtype=str)
            df['Row ID'] = pd.to_numeric(df["Row ID"])
            yield df.sort_values("Row ID")

    @property
    def max_year(self) -> int:
        import pandas as pd
        order_date_series = pd.to_datetime(self.dataframe['Order Date'], format=Randomizer.date_format)
        return order_date_series.max().year

    def _compute_last_orderid_progressive(self) -> int:
        progressive_nums = []
        for old_orderid in self._existing_order_id:
            try:
                # CA-2017-153178
                _, _, prog = old_orderid.split('-')
                progressive_nums.append(int(prog))
            except:
                continue
        if not progressive_nums:
            return 0
        return max(progressive_nums)

    # Row ID
    def row_id(self) -> int:
        self._last_row += 1
        return self._last_row

    # Order id
    def new_order_id(self) -> str:
        def gen_rand_code() -> str:
            return random.choice(("CA", "US"))
        self._last_order_progressive += 1
        progressive = str(self._last_order_progressive).zfill(6)
        return f"{gen_rand_code()}-{self._year}-{progressive}"

    # Order Date
    def rand_order_date(self) -> date:
        start = date(self._year, 1, 1)
        end = date(self._year, 12, 31)
        delta = (end - start).days
        return (
            start
            + timedelta(
                days=random.randint(0, delta)
            )
        )

    # Ship Date
    def rand_ship_delay(self, ship_mode: str) -> timedelta:
        match ship_mode:
            case 'Same Day':
                days = range(0, 2)
                weights=(.95, .05)
            case 'Standard Class':
                days = range(4, 8)
                weights = (.4, .3, .2, .1)
            case 'First Class':
                days = range(1, 4)
                weights = (.22, .39, .39)
            case 'Second Class':
                days = range(2, 6)
                weights = (.39, .2, .2, .31)
        return timedelta(days=random.choices(days, weights)[0])

    # Ship Mode
    def rand_ship_mode(self) -> str:
        return random.choice(
            ('Same Day', 'Standard Class', 'First Class', 'Second Class')
        )

    # Save to a new file
    def create_new_csv_with_data(self) -> None:
        dst_file = self.dst_dir / f"{self._year}.csv"
        mode = 'a' if dst_file.exists() else 'w'
        print(f"Salvataggio su {dst_file}...")
        with open(dst_file, mode=mode, encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.dataframe.columns, lineterminator="\n")
            if mode == "w":
                writer.writeheader()
            i = 0
            while i < self.new_lines:
                order_rows = self.generate_order()
                if not order_rows:
                    continue
                for row in order_rows:
                    writer.writerow(row)
                    i += 1
                    if i >= self.new_lines:
                        break
