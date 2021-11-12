CREATE PROCEDURE property.proc_LoadParcelData(@input_year varchar(4), @input_path varchar(255))
AS
BEGIN
  /*
  Load the parcel data.
  **Have not decided which is the best approach if we want 
  to expand to other counties. Currently this project's scope
  is Los Angeles County, California.

  **Also have not decided whether or not to make table creation
  dynamic. Currently it is manually created for each year.

  How to run it:
  EXEC  proc_LoadParcelData '2019', 'C:\sandbox\scripts\projects\assessor_load\Assessor_Parcels_Data_-_2019.tsv';
  
  Test your output:
  SELECT COUNT(*) FROM property.parcel_data_YYYY
  */

  DECLARE @vcmd varchar(MAX);
  DECLARE @input varchar(255);
  DECLARE @path_file varchar(255);
  DECLARE @tablename varchar(255);
  DECLARE @year varchar(4);
  DECLARE @tableschema varchar(255);
  DECLARE @fulltablename varchar(255);
  

  SET @year = @input_year;
  SET @tableschema = 'property';
  SET @tablename = 'ParcelData_' + @year;
  SET @fulltablename = @tableschema + '.' + @tablename;
  SET @input = @input_path;
  
  IF NOT EXISTS (SELECT
      *
    FROM sysobjects
    WHERE name = @tablename
    AND xtype = 'U')
  BEGIN
  SET @vcmd = 'CREATE TABLE ' + @fulltablename + ' ( 
      zip varchar(50),
      taxRateAreaCity varchar(255),
      ain varchar(255),
      rollYear varchar(4),
      taxRateArea varchar(255),
      assessorID varchar(255),
      propertyLocation varchar(255),
      propertyType varchar(255),
      propertyUseCode varchar(255),
      generalUseType varchar(255),
      specificUseType varchar(255),
      specificUseDetail1 varchar(255),
      specificUsedetail2 varchar(255),
      totBuildingDataLines integer,
      yearBuilt varchar(4),
      effectiveYearBuilt varchar(10),
      sqftMain varchar(50),
      bedrooms float,
      bathrooms float,
      units varchar(50),
      recordingDate varchar(10),
      landValue varchar(255),
      landBaseYear varchar(4),
      improvementValue varchar(255),
      impBaseYear varchar(4),
      totalLandImpValue varchar(255),
      homeownersExemption varchar(255),
      realEstateExemption varchar(255),
      fixtureValue varchar(255),
      fixtureExemption varchar(255),
      personalPropertyValue varchar(255),
      personalPropertyExemption varchar(255),
      isTaxableParcel varchar(2),
      totalValue varchar(255),
      totalExemption varchar(255),
      netTaxableValue varchar(255),
      specialParcelClassification varchar(255),
      adminstrativeRegion varchar(255),
      cluster varchar(255),
      parcelBoundaryDescription varchar(max),
      houseNumber varchar(255),
      houseFraction varchar(255),
      streetDirection varchar(255),
      streetName varchar(255),
      unitNo varchar(255),
      city varchar(255),
      zipcode5 varchar(255),
      rowID bigint NOT NULL PRIMARY KEY,
      centerLat float,
      centerLon float,
      location1 varchar(255)
    );'
	EXECUTE (@vcmd)

    SET @vcmd =
    'BULK INSERT ' +  @fulltablename  + ' FROM ''' +
    @input +
    '''WITH (FIRSTROW =2,FIELDTERMINATOR = ''\t'',ROWTERMINATOR =  ''0x0a'' );'
    EXECUTE (@vcmd)
  END
  ELSE
  BEGIN
    SET @vcmd =
    'BULK INSERT ' +  @fulltablename  + ' FROM ''' +
    @input +
    '''WITH (FIRSTROW =2,FIELDTERMINATOR = ''\t'',ROWTERMINATOR =  ''0x0a'' );'
    EXECUTE (@vcmd)
  END
END;
GO

--EXEC property.proc_LoadParcelData '2019', 'C:\sandbox\scripts\projects\assessor_load\Assessor_Parcels_Data_-_2019.tsv';
--SELECT COUNT(*) FROM property.ParcelData_2019 WHERE taxrateareacity != 'LOS ANGELES'
--SELECT taxrateareacity, COUNT(*) total FROM property.ParcelData_2019 group by taxrateareacity ORDER BY 2 DESC
--TRUNCATE TABLE property.ParcelData_2019
