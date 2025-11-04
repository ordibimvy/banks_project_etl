# 🏦 ETL Pipeline for Global Bank Market Capitalization Data

## 📘 Project Overview
This project is a complete **ETL (Extract, Transform, Load)** pipeline built in Python.  
It extracts data on the world’s largest banks by market capitalization from a live Wikipedia page,  
transforms the values into multiple currencies using exchange rate data, and  
loads the final results into both a **CSV file** and an **SQLite database** for querying and analysis.  

The project was created as part of the *IBM Data Engineering Professional Certificate* on Coursera.

---

## ⚙️ Technologies Used
- **Python 3**
- **Pandas**
- **NumPy**
- **BeautifulSoup (bs4)**
- **SQLite3**
- **Requests**
- **IO / StringIO**
- **Datetime for Logging**

---

## 🧩 ETL Pipeline Stages

### 1️⃣ Extraction
- Retrieves the *“By Market Capitalization”* table from Wikipedia:  
  [List of largest banks – Wikipedia (archived)](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
- Cleans the **Market cap (US$ billion)** column by removing newline characters.
- Renames it to `MC_USD_Billion`.

### 2️⃣ Transformation
- Reads exchange rates from `exchange_rate.csv` and converts them into a dictionary.
- Adds three new columns:
  - `MC_GBP_Billion`
  - `MC_EUR_Billion`
  - `MC_INR_Billion`
- Values are rounded to two decimal places.

### 3️⃣ Loading
- Saves the transformed data to `Largest_banks_data.csv`.
- Loads the same data into a **SQLite** database (`banks.db`) in a table named `Largest_banks`.

### 4️⃣ Querying
Executes and prints results for three SQL queries:
```sql
SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT [Bank name] FROM Largest_banks LIMIT 5;

### Project Structure

├── banks_project.py          # Main ETL script
├── exchange_rate.csv         # Exchange rate reference file
├── Largest_banks_data.csv    # Output CSV file
├── banks.db                  # SQLite database
├── code_log.txt              # Log file for ETL execution
└── README.md                 # Project documentation

### How to run the Project
git clone https://github.com/<your-username>/<your-repo-name>.git
cd <your-repo-name>

### Install the required Libraries
pip install pandas numpy requests beautifulsoup4 lxml

### run the ETL Script
python3 banks_project.py

## 🧾 Author

**Ordi Bimvy Nganzobo**  
🎓 Computer Science & Data Engineering Student at Thomas College  
⚽ Student-Athlete | Data Engineer in Training | AI & Analytics Enthusiast  

📍 Waterville, Maine, USA  
🔗 [LinkedIn](https://www.linkedin.com/in/ordibimvy) • [GitHub](https://github.com/ordibimvy)




