FROM apache/airflow:2.9.2

# Instala dependências de sistema necessárias para compilar pymssql (FreeTDS + build tools)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Retorna para usuário airflow conforme imagem base
USER airflow

# Instala pacotes Python adicionais explicitamente (pymssql pode falhar via var dependendo da ordem)
ENV _PIP_ADDITIONAL_REQUIREMENTS="psycopg2-binary pymssql"
RUN pip install --no-cache-dir psycopg2-binary pymssql || (echo 'Falha pymssql; verifique logs' && exit 1)

