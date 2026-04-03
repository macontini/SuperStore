# Schema — superstore.db

## StrictCopy

**Righe totali:** 25,018

```sql
CREATE TABLE StrictCopy (
	Row_ID 		 NVARCHAR(255) UNIQUE NOT NULL,
	OrderID 	 NVARCHAR(255),
	OrderDate 	 NVARCHAR(255),
	ShipDate 	 NVARCHAR(255),
	ShipMode 	 NVARCHAR(255),
	CustomerID 	 NVARCHAR(255),
	CustomerName NVARCHAR(255),
	Segment 	 NVARCHAR(255),
	Country 	 NVARCHAR(255),
	City 		 NVARCHAR(255),
	State 		 NVARCHAR(255),
	PostalCode   NVARCHAR(255),
	Region 		 NVARCHAR(255),
	ProductID 	 NVARCHAR(255),
	Category 	 NVARCHAR(255),
	SubCategory  NVARCHAR(255),
	ProductName  NVARCHAR(255),
	Sales 		 NVARCHAR(255)

)
```

**Sample (5 righe):**

|   Row_ID | OrderID        | OrderDate   | ShipDate   | ShipMode       | CustomerID   | CustomerName    | Segment   | Country       | City            | State      |   PostalCode | Region   | ProductID       | Category        | SubCategory   | ProductName                                                 |   Sales |
|---------:|:---------------|:------------|:-----------|:---------------|:-------------|:----------------|:----------|:--------------|:----------------|:-----------|-------------:|:---------|:----------------|:----------------|:--------------|:------------------------------------------------------------|--------:|
|        1 | CA-2017-152156 | 08/11/2017  | 11/11/2017 | Second Class   | CG-12520     | Claire Gute     | Consumer  | United States | Henderson       | Kentucky   |        42420 | South    | FUR-BO-10001798 | Furniture       | Bookcases     | Bush Somerset Collection Bookcase                           | 261.96  |
|        2 | CA-2017-152156 | 08/11/2017  | 11/11/2017 | Second Class   | CG-12520     | Claire Gute     | Consumer  | United States | Henderson       | Kentucky   |        42420 | South    | FUR-CH-10000454 | Furniture       | Chairs        | Hon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back | 731.94  |
|        3 | CA-2017-138688 | 12/06/2017  | 16/06/2017 | Second Class   | DV-13045     | Darrin Van Huff | Corporate | United States | Los Angeles     | California |        90036 | West     | OFF-LA-10000240 | Office Supplies | Labels        | Self-Adhesive Address Labels for Typewriters by Universal   |  14.62  |
|        4 | US-2016-108966 | 11/10/2016  | 18/10/2016 | Standard Class | SO-20335     | Sean O'Donnell  | Consumer  | United States | Fort Lauderdale | Florida    |        33311 | South    | FUR-TA-10000577 | Furniture       | Tables        | Bretford CR4500 Series Slim Rectangular Table               | 957.577 |
|        5 | US-2016-108966 | 11/10/2016  | 18/10/2016 | Standard Class | SO-20335     | Sean O'Donnell  | Consumer  | United States | Fort Lauderdale | Florida    |        33311 | South    | OFF-ST-10000760 | Office Supplies | Storage       | Eldon Fold 'N Roll Cart System                              |  22.368 |

---

## Master

**Righe totali:** 25,018

```sql
CREATE TABLE Master (
	Row_ID 		 INTEGER PRIMARY KEY,
	OrderID 	 TEXT NOT NULL,
	OrderDate 	 TEXT NOT NULL,
	ShipDate 	 TEXT,
	ShipMode 	 TEXT NOT NULL,
	CustomerID 	 TEXT NOT NULL,
	CustomerName TEXT NOT NULL,
	Segment 	 TEXT NOT NULL,
	Country 	 TEXT NOT NULL,
	City 		 TEXT NOT NULL,
	State		 TEXT NOT NULL,
	PostalCode 	 TEXT,
	Region 		 TEXT NOT NULL,
	ProductID 	 TEXT NOT NULL,
	Category 	 TEXT NOT NULL,
	SubCategory  TEXT,
	ProductName  TEXT NOT NULL,
	Sales 		 REAL NOT NULL

)
```

**Sample (5 righe):**

|   Row_ID | OrderID        | OrderDate   | ShipDate   | ShipMode       | CustomerID   | CustomerName    | Segment   | Country       | City            | State      |   PostalCode | Region   | ProductID       | Category        | SubCategory   | ProductName                                                 |   Sales |
|---------:|:---------------|:------------|:-----------|:---------------|:-------------|:----------------|:----------|:--------------|:----------------|:-----------|-------------:|:---------|:----------------|:----------------|:--------------|:------------------------------------------------------------|--------:|
|        1 | CA-2017-152156 | 2017-11-08  | 2017-11-11 | Second Class   | CG-12520     | Claire Gute     | Consumer  | United States | Henderson       | Kentucky   |        42420 | South    | FUR-BO-10001798 | Furniture       | Bookcases     | Bush Somerset Collection Bookcase                           | 261.96  |
|        2 | CA-2017-152156 | 2017-11-08  | 2017-11-11 | Second Class   | CG-12520     | Claire Gute     | Consumer  | United States | Henderson       | Kentucky   |        42420 | South    | FUR-CH-10000454 | Furniture       | Chairs        | Hon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back | 731.94  |
|        3 | CA-2017-138688 | 2017-06-12  | 2017-06-16 | Second Class   | DV-13045     | Darrin Van Huff | Corporate | United States | Los Angeles     | California |        90036 | West     | OFF-LA-10000240 | Office Supplies | Labels        | Self-Adhesive Address Labels for Typewriters by Universal   |  14.62  |
|        4 | US-2016-108966 | 2016-10-11  | 2016-10-18 | Standard Class | SO-20335     | Sean O'Donnell  | Consumer  | United States | Fort Lauderdale | Florida    |        33311 | South    | FUR-TA-10000577 | Furniture       | Tables        | Bretford CR4500 Series Slim Rectangular Table               | 957.577 |
|        5 | US-2016-108966 | 2016-10-11  | 2016-10-18 | Standard Class | SO-20335     | Sean O'Donnell  | Consumer  | United States | Fort Lauderdale | Florida    |        33311 | South    | OFF-ST-10000760 | Office Supplies | Storage       | Eldon Fold 'N Roll Cart System                              |  22.368 |

---

## DimProducts

**Righe totali:** 1,861

```sql
CREATE TABLE DimProducts (
	ProductKey 	INTEGER PRIMARY KEY AUTOINCREMENT,
	ProductID 	TEXT UNIQUE,
	ProductName TEXT,
	Category 	TEXT,
	SubCategory TEXT

)
```

**Sample (5 righe):**

|   ProductKey | ProductID       | ProductName                                                 | Category        | SubCategory   |
|-------------:|:----------------|:------------------------------------------------------------|:----------------|:--------------|
|            1 | FUR-BO-10001798 | Bush Somerset Collection Bookcase                           | Furniture       | Bookcases     |
|            2 | FUR-CH-10000454 | Hon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back | Furniture       | Chairs        |
|            3 | OFF-LA-10000240 | Self-Adhesive Address Labels for Typewriters by Universal   | Office Supplies | Labels        |
|            4 | FUR-TA-10000577 | Bretford CR4500 Series Slim Rectangular Table               | Furniture       | Tables        |
|            5 | OFF-ST-10000760 | Eldon Fold 'N Roll Cart System                              | Office Supplies | Storage       |

---

## DimCustomers

**Righe totali:** 1,038

```sql
CREATE TABLE DimCustomers (
	CustomerKey  PRIMARY KEY AUTOINCREMENT,
	CustomerID 	 TEXT NOT NULL,
	CustomerName TEXT,
	Segment 	 TEXT,
	StartDate 	 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	EndDate 	 TEXT DEFAULT NULL,
	IsCurrent 	 INTEGER DEFAULT 1

)
```

**Sample (5 righe):**

|   CustomerKey | CustomerID   | CustomerName    | Segment   | StartDate           | EndDate   |   IsCurrent |
|--------------:|:-------------|:----------------|:----------|:--------------------|:----------|------------:|
|             1 | CG-12520     | Claire Gute     | Consumer  | 2026-04-02 20:54:03 |           |           1 |
|             2 | DV-13045     | Darrin Van Huff | Corporate | 2026-04-02 20:54:03 |           |           1 |
|             3 | SO-20335     | Sean O'Donnell  | Consumer  | 2026-04-02 20:54:03 |           |           1 |
|             4 | BH-11710     | Brosina Hoffman | Consumer  | 2026-04-02 20:54:03 |           |           1 |
|             5 | AA-10480     | Andrew Allen    | Consumer  | 2026-04-02 20:54:03 |           |           1 |

---

## DimGeography

**Righe totali:** 629

```sql
CREATE TABLE DimGeography (
	GeoKey 		INTEGER PRIMARY KEY AUTOINCREMENT,
	PostalCode 	TEXT,
	City 		TEXT,
	State 		TEXT,
	Country 	TEXT,
	Region 		TEXT
)
```

**Sample (5 righe):**

|   GeoKey |   PostalCode | City            | State          | Country       | Region   |
|---------:|-------------:|:----------------|:---------------|:--------------|:---------|
|        1 |        42420 | Henderson       | Kentucky       | United States | South    |
|        2 |        90036 | Los Angeles     | California     | United States | West     |
|        3 |        33311 | Fort Lauderdale | Florida        | United States | South    |
|        4 |        90032 | Los Angeles     | California     | United States | West     |
|        5 |        28027 | Concord         | North Carolina | United States | South    |

---

## FactSales

**Righe totali:** 9,800

```sql
CREATE TABLE FactSales (
	Row_ID 		INTEGER PRIMARY KEY,
	OrderID 	TEXT,
	OrderDate 	TEXT,
	ShipDate 	TEXT,
	ShipMode 	TEXT,
	ProductKey 	INTEGER,
	CustomerKey INTEGER,
	GeoKey 		INTEGER,
	Sales 		REAL,

	FOREIGN KEY (ProductKey) 	REFERENCES DimProducts(ProductKey),
	FOREIGN KEY (CustomerKey) 	REFERENCES DimCustomers(CustomerKey),
	FOREIGN KEY (GeoKey) 		REFERENCES DimGeography(GeoKey)
)
```

**Sample (5 righe):**

|   Row_ID | OrderID        | OrderDate   | ShipDate   | ShipMode       |   ProductKey |   CustomerKey |   GeoKey |   Sales |
|---------:|:---------------|:------------|:-----------|:---------------|-------------:|--------------:|---------:|--------:|
|        1 | CA-2017-152156 | 2017-11-08  | 2017-11-11 | Second Class   |            1 |             1 |        1 | 261.96  |
|        2 | CA-2017-152156 | 2017-11-08  | 2017-11-11 | Second Class   |            2 |             1 |        1 | 731.94  |
|        3 | CA-2017-138688 | 2017-06-12  | 2017-06-16 | Second Class   |            3 |             2 |        2 |  14.62  |
|        4 | US-2016-108966 | 2016-10-11  | 2016-10-18 | Standard Class |            4 |             3 |        3 | 957.577 |
|        5 | US-2016-108966 | 2016-10-11  | 2016-10-18 | Standard Class |            5 |             3 |        3 |  22.368 |

---

