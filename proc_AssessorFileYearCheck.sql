--DROP procedure property.proc_assessorFileYearCheck;
CREATE PROCEDURE property.proc_AssessorFileYearCheck (@input varchar(25))
AS
BEGIN

  /*EXEC property.proc_AssessorFileYearCheck 'YYYY'*/

  DECLARE @file_year varchar(25);

  SET @file_year = @input

  SELECT
    source_file_id,
    formatted_filename
  FROM property.assessorDataView
  WHERE data_year = @file_year;
END;

--EXEC property.proc_AssessorFileYearCheck '2019'