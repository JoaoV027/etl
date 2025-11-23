-- Script de restauração AdventureWorks (ajuste nomes conforme versão do .bak)
-- Copie o arquivo .bak para /var/opt/mssql/backup (diretório mapeado host: ./mssql/backup)
-- Exemplo de nome de arquivo: AdventureWorks2022.bak
-- Este script é idempotente: se o banco já existir, não restaura novamente.

USE master;
GO

-- 1) Descobrir nomes lógicos (descomente a linha abaixo se ainda não souber):
-- RESTORE FILELISTONLY FROM DISK = '/var/opt/mssql/backup/AdventureWorks2022.bak';
-- Copie valores LogicalName para usar em MOVE.

IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name = 'AdventureWorks2022')
BEGIN
    PRINT 'Restaurando banco AdventureWorks2022...';
    RESTORE DATABASE AdventureWorks2022
    FROM DISK = '/var/opt/mssql/backup/AdventureWorks2022.bak'
    WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022.mdf',
         MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_log.ldf',
         REPLACE,
         STATS = 5;
END
ELSE
BEGIN
    PRINT 'Banco AdventureWorks2022 já existe. Pulando restauração.';
END
GO

ALTER DATABASE AdventureWorks2022 SET RECOVERY SIMPLE;
GO
