# AR Hospital Management System

Python-based hospital management system for processing patient billing data, insurance claims, and payment records from SQL Server databases.

## 🚀 Features

- **Database Connectivity**: SQL Server integration using SQLAlchemy and pyodbc
- **Patient Data Processing**: IPD/OPD records with financial analysis
- **Multi-Insurance Support**: PHIC, Company, HMO, and Personal payments
- **Excel Export**: Formatted reports with separate IPD/OPD sheets
- **Date Range Filtering**: Custom date selection for data extraction

## 📦 Prerequisites

- Python 3.8+
- SQL Server 2016+
- ODBC Driver 17 for SQL Server

**Dependencies:**
```
sqlalchemy>=1.4.0
pandas>=1.3.0
openpyxl>=3.0.0
python-dotenv>=0.19.0
pyodbc>=4.0.32
```

## 🔧 Installation

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create `.env` file**
   ```env
   SERVER=your_server_ip\instance_name
   DATABASE=your_database_name
   DB_USERNAME=your_username
   DB_PASSWORD=your_password
   ```

3. **Ensure SQL files exist**
   - `query.sql` - Ma'am Ann's reports
   - `queryD.sql` - Sir Dean's reports

## 🖥️ Usage

```bash
python index.py
```

**Follow prompts:**
- Enter dates (YYYY-MM-DD)
- Select report type: [1]Ma'am Ann [2]Sir Dean
- Choose export: [1]IPD [2]OPD [3]Both

**Output:** Excel files as `2_task1.xlsx`, `2_task2.xlsx`, etc.

## 🗄️ Database Schema

**Required Tables:**
- `HIS_HOSPITALRECORD` - Patient records
- `HIS_BILLINGRECORD` - Billing details
- `AR_IPDOPDPAYMENT/AR_IPDOPDPYMNTDETAIL` - Insurance payments
- `HIS_INSURANCES` - Insurance master data
- `AR_PHICPAYMENTDETAIL` - PhilHealth payments
- `CHRNG_SALESHEADER` - Personal payments
- `HIS_PFRECORD` - Professional fees

## 🔨 Building Executable

```bash
pip install pyinstaller
pyinstaller --clean ar_hospital.spec
```

## 🔧 Troubleshooting

**Connection Issues:**
- Verify server name in `.env`
- Check SQL Server is running
- Ensure port 1433 is open
- Test with SSMS first

**Performance:**
- Add indexes on `DATETIMEDISCHARGED`, `HOSPITALRECORDID`
- Monitor memory usage for large datasets

## 📞 Support

Contact development team for technical support.

---

**Note**: This system handles sensitive medical data. Ensure proper security and compliance measures.