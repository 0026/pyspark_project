# PySpark Project 🚀

A structured **ETL pipeline project built with PySpark**.  
This repository demonstrates how to design, implement, and test scalable data pipelines using Apache Spark in Python.

---

## 📂 Project Structure
```
pyspark_project/ 
├── src/project_pyspark/ # Main source code for ETL pipeline 
├── tests/ # Unit and integration tests 
├── requirements.txt # Python dependencies 
├── pyproject.toml # Project configuration 
├── uv.lock # Lock file for uv package manager 
├── README.md # Project documentation
├── .gitignore # Git ignore rules 
└── .python-version # Python version specification 
```
---


## ⚙️ Requirements

- **Python 3.10+** (see `.python-version`)
- **Apache Spark** (PySpark)
- Dependencies listed in `requirements.txt`

Install dependencies:

```bash
uv pip install -r requirements.txt
```

## ▶️ Usage
Run the main ETL pipeline:
```bash
uv run src/project_pyspark/main.py
```

## 🧪 Testing
Run all tests with:
```bash
uv run pytest tests
```

## 📝 Features
- Modular ETL pipeline with PySpark
- Logging integrated into main and ETL files
- Configurable parameters for flexible execution
- Unit tests for validation and reliability