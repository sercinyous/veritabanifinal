CREATE DATABASE URUN_SATIS


USE URUN_SATIS
GO


Create Table Urunler (
UrunId Int Primary Key Identity (1,1),
UrunAdi nvarchar(250),
UrunAciklama nvarchar(250),
UrunFiyat int
)

Create Table UrunKategori (
KategoriId Int Primary Key Identity (1,1),
KategoriUrunId Int,
KategoriAdi nvarchar(250)
)

Create Table Stok (
StokId Int Primary Key Identity (1,1),
StokUrunId Int,
StokMiktar Int
)

Create Table Sepet (
SepetId Int Primary Key Identity (1,1),
SepetUrunId Int,
SepetMiktar Int
)

insert into Urunler (UrunAdi, UrunAciklama, UrunFiyat) values
('Gömlek', 'Keten Gömlek', 100),
('Pantolon', 'Kumaþ Gömlek', 75),
('Kravat', 'Siyah Çizgili Kravat', 35),
('Ayakkabý', 'Rugan Ayakkabý', 150)

insert into UrunKategori (KategoriUrunId, KategoriAdi) values
(3, 'Kravatlar'),
(2, 'Pantolonlar'),
(1, 'Gömlekler'),
(4, 'Ayakkabýlar')

insert into Stok (StokUrunId, StokMiktar) values
(1, 5),
(4, 2),
(3, 1),
(2, 3)


insert into Sepet (SepetUrunId, SepetMiktar) values
(2, 5),
(3, 4),
(1, 2),
(4, 3)


SELECT Urunler.UrunId, Urunler.UrunId, UrunKategori.KategoriAdi
FROM Urunler
INNER JOIN UrunKategori ON Urunler.UrunId = UrunKategori.KategoriUrunId


SELECT Urunler.UrunId, Urunler.UrunAdi, Sepet.SepetMiktar
FROM Urunler
INNER JOIN Sepet ON Urunler.UrunId = Sepet.SepetUrunId

SELECT Urunler.UrunId, Urunler.UrunAdi, Stok.StokMiktar
FROM Urunler
INNER JOIN Stok ON Urunler.UrunId = Stok.StokUrunId


SELECT 
Urunler.UrunId, 
Urunler.UrunAdi, 
UrunKategori.KategoriAdi, 
Stok.StokMiktar,
Sepet.SepetMiktar
FROM Urunler
INNER JOIN UrunKategori ON Urunler.UrunId = UrunKategori.KategoriUrunId
INNER JOIN Stok ON Urunler.UrunId = Stok.StokUrunId
INNER JOIN Sepet ON Urunler.UrunId = Sepet.SepetUrunId


USE URUN_SATIS
GO

CREATE PROCEDURE tablo_veri_kayit @urun_adi nvarchar(30), @urun_aciklama nvarchar(30), @urun_fiyat int, @urun_kategori nvarchar(30), @stok_miktari int, @sepet_miktari int
AS
INSERT INTO Urunler (UrunAdi,UrunAciklama,UrunFiyat) VALUES (@urun_adi, @urun_aciklama, @urun_fiyat);

DECLARE @son_id int;
SELECT @son_id = IDENT_CURRENT('Urunler');

INSERT INTO UrunKategori(KategoriUrunId, KategoriAdi) VALUES (@son_id, @urun_kategori);

INSERT INTO Stok(StokUrunId, StokMiktar) VALUES (@son_id, @stok_miktari);

INSERT INTO Sepet(SepetUrunId, SepetMiktar) VALUES (@son_id, @sepet_miktari);

CREATE PROCEDURE join_kullanim
AS
SELECT U.UrunAdi, U.UrunAciklama, U.UrunFiyat, S.StokMiktar FROM Urunler as U LEFT JOIN Stok as S ON U.UrunId = S.StokUrunId;


CREATE PROC [bakim_modu]
AS

DECLARE @DatabaseName NVARCHAR(MAX) = 'URUN_SATIS'
DECLARE @IndexName NVARCHAR(MAX)
DECLARE @TableName NVARCHAR(MAX)
DECLARE @CurrentIndexName NVARCHAR(MAX)
DECLARE @CurrentTableName NVARCHAR(MAX)
DECLARE @CmdRebuidIndex NVARCHAR(MAX)

DECLARE @tempIndexTable TABLE
(
	RowID int not null primary key identity(1,1),	
	IndexName NVARCHAR(MAX),
	IndexType NVARCHAR(MAX),
	TableName NVARCHAR(MAX),
	AvgFragmentationInPercent FLOAT,
	ObjectTypeDescription NVARCHAR(MAX)		
)

INSERT INTO @tempIndexTable (IndexName, IndexType, TableName, AvgFragmentationInPercent, ObjectTypeDescription) (
	SELECT i.[name],
	s.[index_type_desc], --s.[index_type_desc]
	o.[name],
	s.[avg_fragmentation_in_percent],
	o.type_desc
	FROM sys.dm_db_index_physical_stats (DB_ID(@DatabaseName), NULL, NULL, NULL, NULL) AS s
	INNER JOIN sys.indexes AS i ON s.object_id = i.object_id AND s.index_id = i.index_id
	INNER JOIN sys.objects AS o ON i.object_id = o.object_id
	WHERE (s.avg_fragmentation_in_percent > 30 and (i.[Name] like '%IX%' OR i.[Name] like '%PK%'))	
)

PRINT 'Indexes to rebuild:'
SELECT * FROM @tempIndexTable;

RETURN; -- Uncomment this line if you want to run the command

DECLARE @totalCount INTEGER
SELECT @totalCount = count(1) FROM @tempIndexTable
DECLARE @counter INTEGER = 1

WHILE(@counter <= @totalCount)
BEGIN	

    SET @CurrentIndexName = (SELECT top 1 IndexName FROM @tempIndexTable WHERE RowID = @counter);
	SET @CurrentTableName = (SELECT top 1 TableName FROM @tempIndexTable WHERE RowID = @counter)
	
	PRINT 'Rebuild starting [' + @CurrentIndexName + 
	'] ON [dbo].[' + @CurrentTableName + '] at ' 
	+ convert(varchar, getdate(), 121)

	BEGIN TRY
		SET @CmdRebuidIndex = 'ALTER INDEX [' + @CurrentIndexName + '] ON [dbo].[' + @CurrentTableName + '] REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = ON, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)'
			EXEC (@CmdRebuidIndex)
			PRINT 'Rebuild executed [' + @CurrentIndexName + '] ON [dbo].[' + @CurrentTableName + '] at ' + convert(varchar, getdate(), 121)
	END TRY
	BEGIN CATCH
		PRINT 'Failed to rebuild [' + @CurrentIndexName + '] ON [dbo].[' + @CurrentTableName + ']'
		PRINT ERROR_MESSAGE()
	END CATCH

	SET @counter += 1;
END
