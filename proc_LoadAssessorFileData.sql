--drop PROCEDURE property.proc_loadAssessorFileData
CREATE PROCEDURE property.proc_LoadAssessorFileData (@path_file varchar(255))

--RETURNS VARCHAR(255) AS
AS
BEGIN

  /*
   Loads Assessor file data.
   EXEC property.proc_LoadAssessorFileData 'C:\sandbox\scripts\sql\realestate\test.txt';
  */

  DECLARE @input varchar(255);
  DECLARE @vcmd varchar(max);
  DECLARE @vcmd1 varchar(max);
  DECLARE @asset_type varchar(10);

  SET @input = @path_file

  CREATE TABLE #temptbl (
    source_file_id varchar(25) PRIMARY KEY,
    source_filename varchar(255),
    asset_type varchar(255)
  );

  SET @vcmd =
  'BULK INSERT  #temptbl FROM ''' +
  @input +
  '''WITH (FIRSTROW =2,FIELDTERMINATOR = ''\t'',ROWTERMINATOR =  ''0x0a'' );'
  EXECUTE (@vcmd)

  IF NOT EXISTS (SELECT
      *
    FROM sysobjects
    WHERE name = 'assessorDataView'
    AND xtype = 'U')
  BEGIN
    SELECT
      source_file_id,
      source_filename,
      LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(asset_type, CHAR(10), CHAR(32)), CHAR(13), CHAR(32)), CHAR(160), CHAR(32)), CHAR(9), CHAR(32)))) AS asset_type,
      SUBSTRING(source_filename, 25, 29) AS data_year,
      REPLACE(source_filename, ' ', '_') AS formatted_filename INTO property.AssessorDataView
    FROM #temptbl
    WHERE source_filename LIKE 'Assessor Parcels Data%';
  END
  ELSE
  BEGIN
    INSERT INTO property.AssessorDataView (source_file_id, source_filename, asset_type, data_year, formatted_filename)
      SELECT
        source_file_id,
        source_filename,
        LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(asset_type, CHAR(10), CHAR(32)), CHAR(13), CHAR(32)), CHAR(160), CHAR(32)), CHAR(9), CHAR(32)))) AS asset_type,
        SUBSTRING(source_filename, 25, 29) AS data_year,
        REPLACE(source_filename, ' ', '_') AS formatted_filename
      FROM #temptbl
      WHERE source_filename LIKE 'Assessor Parcels Data%'
      AND source_file_id NOT IN (SELECT
        source_file_id
      FROM property.AssessorDataView);
    IF @@rowcount = 0
      PRINT 'No new file data found for file ' + @input;
    ELSE
      PRINT 'Sucessfully loaded file: ' + @input;
  END
END;
GO


--SELECT * FROM property.assessorDataView; 
--SELECT replace(viewFileName,' ', '_') FROM property.assessorDataView WHERE year = '2017'; 

