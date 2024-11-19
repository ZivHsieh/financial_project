#!/bin/bash
set -e

# 確保使用正確的 Python 環境
export PATH="/home/airflow/.local/bin:$PATH"
export PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# 等待 MySQL 準備就緒
while ! nc -z mysql 3306; do
    echo "Waiting for MySQL to be ready..."
    sleep 5
done

# 安裝 airflow (如果需要)
pip install apache-airflow

# 初始化數據庫
airflow db init

# 創建管理員用戶
airflow users create \
    --username [airflow admin帳號] \
    --password [airflow admin密碼] \
    --role Admin \
    --email admin@example.com

exit 0
