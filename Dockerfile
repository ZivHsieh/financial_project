FROM apache/airflow:2.7.1

USER root

# 安裝系統套件
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-traditional \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 設置 AIRFLOW_HOME
ENV AIRFLOW_HOME=/opt/airflow

# 創建必要的目錄
RUN mkdir -p ${AIRFLOW_HOME}/dags \
    ${AIRFLOW_HOME}/logs \
    ${AIRFLOW_HOME}/plugins \
    ${AIRFLOW_HOME}/config

# 創建 airflow 用戶和群組（如果不存在）
RUN groupadd -g 50000 airflow || true && \
    useradd -u 50000 -g airflow airflow || true

# 切換到 airflow 用戶安裝 Python 套件
COPY requirements.txt /opt/airflow/
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 複製初始化腳本
USER root
COPY init.sh /init.sh
RUN chmod +x /init.sh

# 確保目錄權限正確
RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

USER airflow
WORKDIR ${AIRFLOW_HOME}
