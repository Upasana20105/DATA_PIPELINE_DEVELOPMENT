# DATA_PIPELINE_DEVELOPMENT

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: UPASANA PRAJAPATI

*INTERN ID*: CT08DF387

*DOMAIN*: DATA SCIENCE

*DURATION*: 8 WEEKS

*MENTOR*: NEELA SANTOSH


*DESCRIPTION OF THE TASK*:

# 🛠️ Salary Data ETL Pipeline

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using **Python** and **pandas** for preprocessing public salary data. It also leverages **scikit-learn pipelines** to transform both numerical and categorical features in a scalable way.

---

## 📁 Files Included

### 📄 `etl_pipeline.py`

* Main script implementing the ETL pipeline.
* Structured as a Python class `SalariesETLPipeline`.
* Steps:

  1. **Extract**: Loads raw data from `Salaries.csv`.
  2. **Transform**:

     * Drops irrelevant columns (`Notes`, `Status`)
     * Converts numeric-like columns (`BasePay`, `OtherPay`, etc.)
     * Handles missing values using imputation.
     * One-hot encodes categorical columns (`JobTitle`, `Agency`)
     * Scales numeric columns.
  3. **Load**: Outputs cleaned and transformed data to `processed_salaries_data.csv`.

> Logging is extensively used for tracking each step and debugging.

---

### 📄 `Salaries.csv`

* Raw input dataset containing salary-related information of public employees.
* Columns include:

  * `Id`, `EmployeeName`, `JobTitle`, `BasePay`, `OvertimePay`, `OtherPay`, `Benefits`, `TotalPay`, `TotalPayBenefits`, `Year`, `Notes`, `Agency`, `Status`.
* Used as the source file for the ETL pipeline.

---

### 📄 `processed_salaries_data.csv`

* Output file generated after running the ETL process.
* Contains:

  * `Id`, `EmployeeName` (untouched)
  * Transformed numerical columns
  * One-hot encoded categorical columns (over **2,100** new feature columns).
* Cleaned and ready for ML model input or further analysis.

---

### 🖼️ `process1.png` to `process5.png`

* **Screenshots** of the ETL pipeline execution in **VS Code**.
* Show:

  * Data extraction & initial info
  * Cleaning & feature conversion
  * Transformation using pipelines
  * Transformed data preview
  * Final verification of saved file

---

## ✅ How to Run

1. Install requirements:

   ```bash
   pip install pandas scikit-learn
   ```
2. Place `Salaries.csv` in the same directory.
3. Run the pipeline:

   ```bash
   python etl_pipeline.py
   ```
4. Output will be saved as `processed_salaries_data.csv`.

---

## 📌 Features

* Clean and modular ETL architecture using OOP
* Handles missing values and incorrect types
* Uses `ColumnTransformer` for efficient preprocessing
* Logs every key step with `logging`
* Final transformed dataset is ready for modeling

---

## 📷 Sample Output Preview

A few transformed columns:

```plaintext
Id  EmployeeName     cat__JobTitle_ZOO CURATOR  cat__Agency_San Francisco  ...
1   NATHANIEL FORD         1.0                      1.0
2   GARY JIMENEZ           1.0                      1.0
...
```


