#!/bin/bash
set -euo pipefail

# Instala mssql-tools18 (novo caminho /opt/mssql-tools18/bin/sqlcmd)
# Só precisa rodar uma vez. Container deve ter acesso à internet.

if command -v sqlcmd >/dev/null 2>&1; then
  echo "sqlcmd já disponível. Saindo."; exit 0
fi

apt-get update -y
apt-get install -y curl gnupg apt-transport-https ca-certificates
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update -y
ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev
ln -s /opt/mssql-tools18/bin/sqlcmd /usr/bin/sqlcmd

echo "sqlcmd instalado com sucesso. Versão:";
sqlcmd -? | head -n 1 || true
