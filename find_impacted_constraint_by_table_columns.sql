

-- Step 1 - Collect all impacted column in temp table
-- Step 2 - Create back up all impacted foreign constraints to recreate later
-- Step 3 - Create back up all impacted primary/unique constraints to recreate later
-- Step 4 - Create back up all impacted Indexes to recreate later
-- Step 5 - Create back up all impacted stats to recreate later
-- Step 6 - Create back up all impacted default constraints to recreate later

DECLARE @run INT = 1;-- 0 or 1
DECLARE @dateRun DATETIME = GETDATE();

-- Step 1 - Collect all impacted column in temp table
IF OBJECT_ID('tempdb..#temp_impacted_columns') IS NOT NULL
BEGIN
	DROP TABLE #temp_impacted_columns;
END

CREATE TABLE #temp_impacted_columns (
	[table_id] INT,
	[table_name] SYSNAME,
	[column_id] INT,
	[column_name] SYSNAME,
	)


--Insert into #temp_impacted_columns impacted columns based on criteria
-- In this example column name is code with certain type
INSERT INTO #temp_impacted_columns (
	[table_id],
	[table_name],
	column_id,
	[column_name]
	)
SELECT A.parent_object_id AS table_id,
	OBJECT_NAME(A.parent_object_id) AS table_name,
	C.column_id AS column_id,
	C.name AS column_name
FROM sys.foreign_keys A WITH (NOLOCK)
INNER JOIN sys.foreign_key_columns B WITH (NOLOCK) ON B.constraint_object_id = A.object_id
INNER JOIN sys.columns C WITH (NOLOCK) ON B.parent_object_id = C.object_id
	AND B.parent_column_id = C.column_id
INNER JOIN sys.columns D WITH (NOLOCK) ON B.referenced_object_id = D.object_id
	AND B.referenced_column_id = D.column_id
WHERE D.name = 'code'
	AND OBJECT_NAME(A.referenced_object_id) = 'country'
	AND C.max_length = 4
	AND C.system_type_id IN (
		175,
		167
		)

INSERT INTO #temp_impacted_columns (
	[table_id],
	[table_name],
	column_id,
	[column_name]
	)
SELECT C.object_id AS table_id,
	OBJECT_NAME(C.object_id) AS table_name,
	C.column_id AS column_id,
	C.name AS column_name
FROM sys.columns C WITH (NOLOCK)
INNER JOIN sys.objects o WITH (NOLOCK) ON C.object_id = o.object_id
WHERE C.max_length = 4
	AND C.system_type_id IN (
		175,
		167
		)
	AND o.type = 'U'
	AND (
		C.name LIKE '%CODE%'
		OR C.name LIKE '%COUNTRY%'
		)
	AND NOT EXISTS (
		SELECT 1
		FROM #temp_impacted_columns
		WHERE table_id = C.object_id
			AND C.column_id = C.column_id
		)

-- Step 2 - Create back up all impacted foreign constraints to recreate later
IF (
		NOT EXISTS (
			SELECT *
			FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_NAME = 'temp_constraints'
			)
		)
BEGIN
	CREATE TABLE temp_constraints (
		[object_id] INT,
		[table_name] SYSNAME,
		[constraint_name] SYSNAME,
		[column_name] SYSNAME,
		[file_group_name] SYSNAME,
		[constraint_type] NVARCHAR(10),
		[constraint_create_stmt] NVARCHAR(MAX),
		[constraint_drop_stmt] NVARCHAR(MAX),
		[run] INT DEFAULT 0,
		[dateRun] DATETIME,
		[dropOrder] INT DEFAULT 1000,
		[createOrder] INT DEFAULT 1000,
		)
END

SELECT @run = COALESCE((MAX(run) + 1), 1)
FROM temp_constraints

--Step 1 - Take Backup of all FK constraints to execute later
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	constraint_type,
	constraint_create_stmt,
	constraint_drop_stmt,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT A.parent_object_id AS objectId,
	OBJECT_NAME(A.parent_object_id) AS table_name,
	A.name AS constraint_name,
	C.name AS column_name,
	'' AS file_group_name,
	'FK' AS constraint_type,
	CAST('IF (OBJECT_ID(''' + QUOTENAME(A.name) + ''', ''F'') IS NULL) BEGIN ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(A.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(A.parent_object_id)) + CASE is_not_trusted
			WHEN 0
				THEN ' WITH CHECK '
			ELSE ' WITH NOCHECK '
			END + ' ADD CONSTRAINT ' + QUOTENAME(A.name) + ' FOREIGN KEY (' + QUOTENAME(C.name) + ') REFERENCES ' + QUOTENAME(OBJECT_SCHEMA_NAME(A.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(A.referenced_object_id)) + ' (' + D.name + ')' + ' ON UPDATE ' + CASE A.update_referential_action
			WHEN 0
				THEN 'NO ACTION '
			WHEN 1
				THEN 'CASCADE '
			WHEN 2
				THEN 'SET NULL '
			ELSE 'SET DEFAULT '
			END + ' ON DELETE ' + CASE A.delete_referential_action
			WHEN 0
				THEN 'NO ACTION '
			WHEN 1
				THEN 'CASCADE '
			WHEN 2
				THEN 'SET NULL '
			ELSE 'SET DEFAULT '
			END + CASE A.is_not_for_replication
			WHEN 1
				THEN ' NOT FOR REPLICATION '
			ELSE ''
			END + ';' + CHAR(13) + 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(A.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(A.parent_object_id)) + CASE A.is_disabled
			WHEN 0
				THEN ' CHECK '
			ELSE ' NOCHECK '
			END + 'CONSTRAINT ' + QUOTENAME(A.name) + ' END' AS NVARCHAR(MAX)) AS constraint_create_stmt,
	CAST('IF (OBJECT_ID(''' + QUOTENAME(A.name) + ''', ''F'') IS NOT NULL) BEGIN ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(A.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(A.parent_object_id)) + ' DROP CONSTRAINT ' + QUOTENAME(A.name) + ' END;' AS NVARCHAR(MAX)) AS constraint_drop_stmt,
	@run,
	@dateRun,
	1,
	2
FROM sys.foreign_keys A
INNER JOIN sys.foreign_key_columns B ON B.constraint_object_id = A.object_id
INNER JOIN sys.columns C ON B.parent_object_id = C.object_id
	AND B.parent_column_id = C.column_id
INNER JOIN sys.columns D ON B.referenced_object_id = D.object_id
	AND B.referenced_column_id = D.column_id
WHERE D.name = 'code'
	AND OBJECT_NAME(A.referenced_object_id) = 'country'
	AND C.max_length = 4
	AND C.system_type_id IN (
		175,
		167
		)

-- Step 3 - Create back up all impcated primary/unique constraints to recreate later
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	constraint_type,
	constraint_create_stmt,
	constraint_drop_stmt,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT A.[object_id],
	OBJECT_NAME(A.[object_id]) AS Table_Name,
	A.[Name] AS Index_Name,
	G.column_name,
	C.name AS FileGroupName,
	'PK/UQ',
	CAST('IF (INDEXPROPERTY (' + CAST(A.object_id AS NVARCHAR(MAX)) + ', ''' + A.Name + ''' , ''IndexID'' )  IS NULL) BEGIN ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(A.object_id)) + '.' + QUOTENAME(OBJECT_NAME(A.object_id)) + ' ADD  CONSTRAINT ' + QUOTENAME(A.[Name]) + CASE 
			WHEN A.type = 1
				AND is_primary_key = 1
				THEN ' PRIMARY KEY CLUSTERED '
			WHEN A.type = 2
				AND is_primary_key = 1
				THEN ' PRIMARY KEY NONCLUSTERED '
			WHEN A.type = 1
				AND is_unique_constraint = 1
				THEN ' UNIQUE CLUSTERED '
			WHEN A.type = 2
				AND is_unique_constraint = 1
				THEN ' UNIQUE NONCLUSTERED '
			ELSE ' UNKNOWN '
			END + ' (' + Stuff((
				SELECT ',[' + COL_NAME(A.[object_id], C.column_id) + CASE 
						WHEN C.is_descending_key = 1
							THEN '] Desc'
						ELSE '] Asc'
						END
				FROM sys.index_columns C WITH (NOLOCK)
				WHERE A.[Object_ID] = C.object_id
					AND A.Index_ID = C.Index_ID
					AND C.is_included_column = 0
				ORDER BY C.key_Ordinal ASC
				FOR XML Path('')
				), 1, 1, '') + ') ' + CASE 
			WHEN A.type = 1
				THEN ''
			ELSE Coalesce('Include (' + Stuff((
							SELECT ',' + QUOTENAME(COL_NAME(A.[object_id], C.column_id))
							FROM sys.index_columns C WITH (NOLOCK)
							WHERE A.[Object_ID] = C.object_id
								AND A.Index_ID = C.Index_ID
								AND C.is_included_column = 1
							ORDER BY C.index_column_id ASC
							FOR XML Path('')
							), 1, 1, '') + ') ', '')
			END + CASE 
			WHEN A.has_filter = 1
				THEN 'Where ' + A.filter_definition
			ELSE ''
			END + ' With ( SORT_IN_TEMPDB = OFF' + CASE 
			WHEN fill_factor > 0
				THEN ', Fillfactor = ' + Cast(fill_factor AS VARCHAR(3))
			ELSE ''
			END + CASE 
			WHEN A.[is_padded] = 1
				THEN ', PAD_INDEX = ON'
			ELSE ', PAD_INDEX = OFF'
			END + CASE 
			WHEN D.[no_recompute] = 1
				THEN ', STATISTICS_NORECOMPUTE = ON'
			ELSE ', STATISTICS_NORECOMPUTE = OFF'
			END + CASE 
			WHEN A.[ignore_dup_key] = 1
				THEN ', IGNORE_DUP_KEY = ON'
			ELSE ', IGNORE_DUP_KEY = OFF'
			END + CASE 
			WHEN A.[ALLOW_ROW_LOCKS] = 1
				THEN ', ALLOW_ROW_LOCKS = ON'
			ELSE ', ALLOW_ROW_LOCKS = OFF'
			END + CASE 
			WHEN A.[ALLOW_PAGE_LOCKS] = 1
				THEN ', ALLOW_PAGE_LOCKS = ON'
			ELSE ', ALLOW_PAGE_LOCKS = OFF'
			END + CASE 
			WHEN P.[data_compression] = 0
				THEN ', DATA_COMPRESSION = NONE'
			WHEN P.[data_compression] = 1
				THEN ', DATA_COMPRESSION = ROW'
			ELSE ', DATA_COMPRESSION = PAGE'
			END + ') On ' + CASE 
			WHEN C.type = 'FG'
				THEN QUOTENAME(C.name)
			ELSE QUOTENAME(C.name) + '(' + F.Partition_Column + ')'
			END + ' END; ' AS NVARCHAR(Max)) AS Index_Create_Statement,
	'IF (INDEXPROPERTY (' + CAST(A.object_id AS NVARCHAR(MAX)) + ', ''' + A.Name + ''' , ''IndexID'' )  IS NOT NULL) BEGIN ALTER TABLE ' + QUOTENAME(S.name) + '.' + QUOTENAME(OBJECT_NAME(A.[object_id])) + ' DROP CONSTRAINT ' + QUOTENAME(A.[Name]) + ' WITH ( ONLINE = OFF ) END;' AS Index_Drop_Statement,
	@run,
	@dateRun,
	2,
	1
FROM SYS.Indexes A WITH (NOLOCK)
INNER JOIN sys.objects B WITH (NOLOCK) ON A.object_id = B.object_id
INNER JOIN SYS.schemas S ON B.schema_id = S.schema_id
INNER JOIN SYS.data_spaces C WITH (NOLOCK) ON A.data_space_id = C.data_space_id
INNER JOIN SYS.stats D WITH (NOLOCK) ON A.object_id = D.object_id
	AND A.index_id = D.stats_id
INNER JOIN (
	SELECT object_id,
		index_id,
		Data_Compression,
		ROW_NUMBER() OVER (
			PARTITION BY object_id,
			index_id ORDER BY COUNT(*) DESC
			) AS Main_Compression
	FROM sys.partitions WITH (NOLOCK)
	GROUP BY object_id,
		index_id,
		Data_Compression
	) P ON A.object_id = P.object_id
	AND A.index_id = P.index_id
	AND P.Main_Compression = 1
OUTER APPLY (
	SELECT COL_NAME(A.object_id, E.column_id) AS Partition_Column
	FROM sys.index_columns E WITH (NOLOCK)
	WHERE E.object_id = A.object_id
		AND E.index_id = A.index_id
		AND E.partition_ordinal = 1
	) F
CROSS APPLY (
	SELECT column_name
	FROM (
		SELECT (
				STUFF((
						SELECT CONCAT (
								'|',
								tc.column_name
								)
						FROM #temp_impacted_columns tc
						INNER JOIN sys.index_columns IC WITH (NOLOCK) ON tc.column_id = IC.column_id
							AND tc.table_id = IC.object_id
						INNER JOIN sys.objects B WITH (NOLOCK) ON tc.table_id = B.object_id
						WHERE IC.index_id = A.index_id
							AND tc.table_id = A.object_id
						FOR XML PATH('')
						), 1, 1, '')
				) AS column_name
		) t
	WHERE t.column_name IS NOT NULL
	) G
WHERE A.type IN (
		1,
		2
		) --clustered and nonclustered
	AND (
		A.is_unique_constraint = 1
		OR A.is_primary_key = 1
		)
	AND B.Type != 'S'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'queue_messages_%'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'filestream_tombstone_%'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'sys%'

-- Step 4 - Create back up all impacted Indexes to recreate later
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	constraint_type,
	constraint_create_stmt,
	constraint_drop_stmt,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT A.[object_id],
	OBJECT_NAME(A.[object_id]) AS Table_Name,
	A.[Name] AS Index_Name,
	G.column_name,
	C.name AS FileGroupName,
	'INDEX',
	CAST('IF (INDEXPROPERTY (' + CAST(A.object_id AS NVARCHAR(MAX)) + ', ''' + A.Name + ''' , ''IndexID'' )  IS NULL) BEGIN ' + CASE 
			WHEN A.type = 1
				AND is_unique = 1
				THEN 'Create Unique Clustered Index '
			WHEN A.type = 1
				AND is_unique = 0
				THEN 'Create Clustered Index '
			WHEN A.type = 2
				AND is_unique = 1
				THEN 'Create Unique NonClustered Index '
			WHEN A.type = 2
				AND is_unique = 0
				THEN 'Create NonClustered Index '
			ELSE 'UKNOWN '
			END + QUOTENAME(A.[Name]) + ' On ' + QUOTENAME(S.name) + '.' + QUOTENAME(OBJECT_NAME(A.[object_id])) + ' (' + Stuff((
				SELECT ',[' + COL_NAME(A.[object_id], C.column_id) + CASE 
						WHEN C.is_descending_key = 1
							THEN '] Desc'
						ELSE '] Asc'
						END
				FROM sys.index_columns C WITH (NOLOCK)
				WHERE A.[Object_ID] = C.object_id
					AND A.Index_ID = C.Index_ID
					AND C.is_included_column = 0
				ORDER BY C.key_Ordinal ASC
				FOR XML Path('')
				), 1, 1, '') + ') ' + CASE 
			WHEN A.type = 1
				THEN ''
			ELSE Coalesce('Include (' + Stuff((
							SELECT ',' + QUOTENAME(COL_NAME(A.[object_id], C.column_id))
							FROM sys.index_columns C WITH (NOLOCK)
							WHERE A.[Object_ID] = C.object_id
								AND A.Index_ID = C.Index_ID
								AND C.is_included_column = 1
							ORDER BY C.index_column_id ASC
							FOR XML Path('')
							), 1, 1, '') + ') ', '')
			END + CASE 
			WHEN A.has_filter = 1
				THEN 'Where ' + A.filter_definition
			ELSE ''
			END + ' With ( SORT_IN_TEMPDB = OFF' + CASE 
			WHEN A.fill_factor > 0
				THEN ', Fillfactor = ' + Cast(fill_factor AS VARCHAR(3))
			ELSE ''
			END + CASE 
			WHEN A.[is_padded] = 1
				THEN ', PAD_INDEX = ON'
			ELSE ', PAD_INDEX = OFF'
			END + CASE 
			WHEN D.[no_recompute] = 1
				THEN ', STATISTICS_NORECOMPUTE = ON'
			ELSE ', STATISTICS_NORECOMPUTE = OFF'
			END + CASE 
			WHEN A.[ignore_dup_key] = 1
				THEN ', IGNORE_DUP_KEY = ON'
			ELSE ', IGNORE_DUP_KEY = OFF'
			END + CASE 
			WHEN A.[ALLOW_ROW_LOCKS] = 1
				THEN ', ALLOW_ROW_LOCKS = ON'
			ELSE ', ALLOW_ROW_LOCKS = OFF'
			END + CASE 
			WHEN A.[ALLOW_PAGE_LOCKS] = 1
				THEN ', ALLOW_PAGE_LOCKS = ON'
			ELSE ', ALLOW_PAGE_LOCKS = OFF'
			END + CASE 
			WHEN P.[data_compression] = 0
				THEN ', DATA_COMPRESSION = NONE'
			WHEN P.[data_compression] = 1
				THEN ', DATA_COMPRESSION = ROW'
			ELSE ', DATA_COMPRESSION = PAGE'
			END + ') On ' + CASE 
			WHEN C.type = 'FG'
				THEN QUOTENAME(C.name)
			ELSE QUOTENAME(C.name) + '(' + F.Partition_Column + ')'
			END + ' END; ' AS NVARCHAR(Max)) AS Index_Create_Statement,
	'IF (INDEXPROPERTY (' + CAST(A.object_id AS NVARCHAR(MAX)) + ', ''' + A.Name + ''' , ''IndexID'' )  IS NOT NULL) BEGIN DROP INDEX ' + QUOTENAME(A.[Name]) + ' On ' + QUOTENAME(S.name) + '.' + QUOTENAME(OBJECT_NAME(A.[object_id])) + ' END;' AS Index_Drop_Statement,
	@run,
	@dateRun,
	3,
	3
FROM SYS.Indexes A WITH (NOLOCK)
INNER JOIN sys.objects B WITH (NOLOCK) ON A.object_id = B.object_id
INNER JOIN SYS.schemas S ON B.schema_id = S.schema_id
INNER JOIN SYS.data_spaces C WITH (NOLOCK) ON A.data_space_id = C.data_space_id
INNER JOIN SYS.stats D WITH (NOLOCK) ON A.object_id = D.object_id
	AND A.index_id = D.stats_id
INNER JOIN
	(
	SELECT object_id,
		index_id,
		Data_Compression,
		ROW_NUMBER() OVER (
			PARTITION BY object_id,
			index_id ORDER BY COUNT(*) DESC
			) AS Main_Compression
	FROM sys.partitions WITH (NOLOCK)
	GROUP BY object_id,
		index_id,
		Data_Compression
	) P ON A.object_id = P.object_id
	AND A.index_id = P.index_id
	AND P.Main_Compression = 1
OUTER APPLY (
	SELECT COL_NAME(A.object_id, E.column_id) AS Partition_Column
	FROM sys.index_columns E WITH (NOLOCK)
	WHERE E.object_id = A.object_id
		AND E.index_id = A.index_id
		AND E.partition_ordinal = 1
	) F
CROSS APPLY (
	SELECT column_name
	FROM (
		SELECT (
				STUFF((
						SELECT CONCAT (
								'|',
								tc.column_name
								)
						FROM #temp_impacted_columns tc
						INNER JOIN sys.index_columns IC WITH (NOLOCK) ON tc.column_id = IC.column_id
							AND tc.table_id = IC.object_id
						INNER JOIN sys.objects B WITH (NOLOCK) ON tc.table_id = B.object_id
						WHERE IC.index_id = A.index_id
							AND tc.table_id = A.object_id
						FOR XML PATH('')
						), 1, 1, '')
				) AS column_name
		) t
	WHERE t.column_name IS NOT NULL
	) G
WHERE A.type IN (
		1,
		2
		) --clustered and nonclustered
	AND A.is_unique_constraint = 0
	AND A.is_primary_key = 0
	AND B.Type != 'S'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'queue_messages_%'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'filestream_tombstone_%'
	AND OBJECT_NAME(A.[object_id]) NOT LIKE 'sys%'

-- Step 5 - Create back up all impacted stats to recreate later
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	constraint_type,
	constraint_create_stmt,
	constraint_drop_stmt,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT DISTINCT s.[object_id] AS object_id,
	OBJECT_NAME(s.[object_id]) AS TableName,
	s.name AS stat_name,
	tc.column_name,
	'' AS file_group_name,
	'STAT',
	CASE 
		WHEN s.user_created = 1
			THEN 'IF (INDEXPROPERTY (' + CAST(s.object_id AS NVARCHAR(MAX)) + ', ''' + s.[Name] + ''' , ''IsStatistics'' ) IS NULL) BEGIN CREATE STATISTICS ' + QUOTENAME(S.NAME) + ' ON ' + QUOTENAME(SCHEMA_NAME(obj.schema_id)) + '.' + QUOTENAME(obj.[name]) + '(' + STUFF((
						SELECT ',' + QUOTENAME(c.name)
						FROM sys.stats_columns sc
						INNER JOIN sys.columns c ON c.[object_id] = sc.[object_id]
							AND c.column_id = sc.column_id
						WHERE sc.[object_id] = s.[object_id]
							AND sc.stats_id = s.stats_id
						ORDER BY sc.stats_column_id
						FOR XML PATH('')
						), 1, 1, '') + ')' + ISNULL(' WHERE ' + filter_definition, '') + ISNULL(STUFF(CASE 
							WHEN no_recompute = 1
								THEN ',NORECOMPUTE'
							ELSE ''
							END, 1, 1, ' WITH '), '') + ' END;'
		ELSE ''
		END AS constraint_create_stmt,
	'IF (INDEXPROPERTY (' + CAST(s.object_id AS NVARCHAR(MAX)) + ', ''' + s.[Name] + ''' , ''IsStatistics'' ) IS NOT NULL) BEGIN DROP STATISTICS ' + QUOTENAME(OBJECT_NAME(s.[object_id])) + '.' + QUOTENAME(s.name) + ' END;' AS constraint_drop_stmt,
	@run,
	@dateRun,
	4,
	4
FROM sys.stats s
INNER JOIN sys.stats_columns sc ON sc.[object_id] = s.[object_id]
	AND sc.stats_id = s.stats_id
INNER JOIN #temp_impacted_columns tc ON tc.table_id = sc.[object_id]
	AND tc.column_id = sc.column_id
INNER JOIN sys.partitions par ON par.[object_id] = s.[object_id]
INNER JOIN sys.objects obj ON par.[object_id] = obj.[object_id]
WHERE OBJECTPROPERTY(s.OBJECT_ID, 'IsUserTable') = 1
	AND (
		s.auto_created = 1
		OR s.user_created = 1
		)

-- Step 6 - Create back up all impacted default constraints to recreate later
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	constraint_type,
	constraint_create_stmt,
	constraint_drop_stmt,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT ac.object_id,
	tc.table_name,
	dc.name AS constraint_name,
	ac.name AS column_name,
	'' AS file_group_name,
	'DF' AS constraint_type,
	CAST('IF (OBJECT_ID(''' + QUOTENAME(dc.name) + ''', ''D'') IS NULL) BEGIN ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(ac.object_id)) + '.' + QUOTENAME(tc.table_name) + ' ADD CONSTRAINT ' + QUOTENAME(dc.name) + ' DEFAULT ' + DC.DEFINITION + ' FOR ' + QUOTENAME(ac.name) + ' END;' AS NVARCHAR(MAX)) AS constraint_create_stmt,
	CAST('IF (OBJECT_ID(''' + QUOTENAME(dc.name) + ''', ''D'') IS NOT NULL) BEGIN ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(ac.object_id)) + '.' + QUOTENAME(tc.table_name) + ' DROP CONSTRAINT ' + QUOTENAME(dc.name) + ' END;' AS NVARCHAR(MAX)) AS constraint_drop_stmt,
	@run,
	@dateRun,
	5,
	5
FROM sys.all_columns ac
INNER JOIN #temp_impacted_columns tc ON ac.object_id = tc.table_id
	AND ac.column_id = tc.column_id
INNER JOIN sys.default_constraints dc ON ac.default_object_id = dc.object_id

--Insert any additional table/column combo from temp table 
INSERT INTO temp_constraints (
	object_id,
	table_name,
	constraint_name,
	column_name,
	file_group_name,
	run,
	dateRun,
	dropOrder,
	createOrder
	)
SELECT tc.table_id,
	tc.table_name,
	'' AS constraint_name,
	tc.column_name,
	'' AS file_group_name,
	@run,
	@dateRun,
	5,
	5
FROM #temp_impacted_columns tc
WHERE NOT EXISTS (
		SELECT 1
		FROM temp_constraints tcc
		WHERE tc.table_id = tcc.object_id
			AND tc.column_name = tcc.column_name
		)

