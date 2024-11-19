# DIY ETF Investment Portfolio Dashboard
The code was written by 

## Purpose  
When investing, the following concerns arise:  
- Not wanting to research the internal components of ETFs.  
- Wanting to identify assets with stable dividends and consistent fill rates.  
- Also wanting to know if the financial ratios of these assets are solid and if there are investment risks.  
So we create a dashboard to address the above concerns and organize our own high-yield ETFs. The goal is to achieve automatic data updates, eliminating the need for manual collection of raw data and report generation. This dashboard will identify high-yield and stable fill-rate assets while simultaneously displaying their financial ratio performance to confirm investment risks.  

## 🎯 Key Features

### 1. Multi-dimensional Analysis
- Industry dividend yield analysis
- Dividend coverage tracking
- Financial ratio evaluation
- Industry trend analysis

### 2. Automated Updates
- Automated data crawling
- Automated data cleaning
- Automated report updates

## 🛠 Technical Architecture
![螢幕擷取畫面 2024-11-19 185740](https://github.com/user-attachments/assets/b187277a-9b36-40e8-898b-1122e0ce7901)
### Core Technologies
- Python
- Docker
- Apache Airflow
- Tableau
- Google Cloud Platform

### Data Management
- MySQL Database
- ETL Process Automation
- Data Cleaning and Transformation

## 📈 Data Flow

Data Source → Web Crawling → Data Cleaning → Database → Visualization

### Data Pipeline
Our data pipeline is divided into the following three parts：
1. Financial Ratio Module
   - Balance Sheet → debt_ratios
   - Income Statement → income_ratio
   - Cash Flow Statement → cashflow_ratio
   
2. Company Basic Information Module
   - Company Information → company_info
   - Industry Classification → company_info_join_industry
   
3. Dividend Yield and Coverage Analysis Module
   - Stock Price Data → 
   - Dividend Data → 
   - Dividend Coverage Tracking → 

## 🔍 Key Findings

1. Industry Analysis Results:
   - Industries with high dividend yields typically have longer dividend coverage periods
   - Construction Materials and Biotech/Medical industries require special attention to financial indicators
   
2. Quality Investment Characteristics:
   - Stable dividend and dividend coverage
   - Healthy financial ratios
   - Promising industry outlook

## 👥 Team Members

- Team Leader: Cheng-Chi Hsieh
- Members:
  - Hsueh-Shih Lin
  - Min Weng
  - Bo-Sheng Hsu
  - Yu-Ning Cheng
- Academic Advisors:
  - Jo-Yu Chang
  - Cheng-You Shih

### Installation

[Add installation instructions here]

### Usage

[Add usage instructions here]

### Contributing

[Add contribution guidelines here]
