\# Data Dictionary



\## Bronze Layer (Raw Zone)

\### raw\_books

\- `source` (SourceType): Data source identifier

\- `source\_url` (str): URL where data was fetched from

\- `raw\_content` (str): Raw HTML/JSON content

\- `ingested\_at` (datetime): Timestamp of ingestion



\## Silver Layer (Clean Zone)  

\### clean\_books

\- `source\_id` (str): Unique identifier from source system

\- `isbn13` (str, optional): International Standard Book Number (13 digits)

\- `title` (str): Book title (1-500 characters)

\- `authors` (List\[str]): List of authors

\- `price\_current` (float): Current price (0.0 - 10000.0)

\- `currency` (CurrencyType): Currency code (USD, GBP, EUR)



\## Gold Layer (Curated Zone)

\### dim\_book (SCD Type 2)

\- `book\_id` (str): System-generated surrogate key

\- `isbn13` (str, optional): Natural key when available

\- `valid\_from`/`valid\_to` (datetime): Version validity period

\- `is\_current` (bool): Flag for current version



\### fact\_price\_history

\- `book\_id` (str): Foreign key to dim\_book

\- `price` (float): Price at effective\_date

\- `effective\_date` (datetime): When this price was effective



\### fact\_availability

\- `book\_id` (str): Foreign key to dim\_book  

\- `status` (AvailabilityStatus): Stock status

\- `effective\_date` (datetime): When this status was recorded

