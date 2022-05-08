
/****** Object:  StoredProcedure [playpen].[p_url_splitting]    Script Date: 02/04/2022 19:05:14 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

alter PROC [playpen].[FF_external_to_internal_import] AS
-- exec [playpen].[FF_external_to_internal_import]

--------------------------------------------------------------------------------------------------------------------------------------
-- Start of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN
BEGIN TRY

SET NOCOUNT ON;



--------------------------------------------------------------------------------------------------------------------------------------
-- Log Start
--------------------------------------------------------------------------------------------------------------------------------------

DECLARE	@Rec_Count         BigInt;

DECLARE @Batch_No_PV       Uniqueidentifier,	--PV to indicate passing value
        @Proc_Name_PV      VarChar(100),
        @Proc_Call_PV      VarChar(1000),
        @Error_Detail_PV   VarChar(8000),
        @Step_Name_PV      VarChar(500);

SELECT @Batch_No_PV    = NEWID(),
       @Proc_Name_PV   = '[playpen].[FF_external_to_internal_import]',
       @Proc_Call_PV   = 'EXEC [playpen].[FF_external_to_internal_import]';
	   
EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'STARTED',
                          @Error_Detail   = NULL;


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 1 - Bring in the python-generated cpai data from blob into the external table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 1 - Bring in the python-generated cpai data from blob into the external table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


--drop external table playpen.jm_fixedfees_cpa_increases_ext
--CREATE EXTERNAL TABLE playpen.jm_fixedfees_cpa_increases_ext
--(
--	advertiser nvarchar(500)
--	,oldRate nvarchar(250)
--	,newRate nvarchar(250)
--	,liveDate nvarchar(250)
--	,endDate nvarchar(250)
--	,direct nvarchar(250)
--)
--WITH (DATA_SOURCE = accurankerIngest,LOCATION = N'FixedFees/cpa',FILE_FORMAT = [myradiotimesonboardingmetadata],REJECT_TYPE = VALUE,REJECT_VALUE = 1)



--------------------------------------------------------------------------------------------------------------------------------------
-- Step 2 - put the new cpai data into the internal table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 2 - put the new cpai data into the internal table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


truncate table playpen.jm_fixedfees_cpa_increases
insert into playpen.jm_fixedfees_cpa_increases
select * from playpen.jm_fixedfees_cpa_increases_ext


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 3 - Bring in the python-generated bookings data from blob into the external table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 3 - Bring in the python-generated bookings data from blob into the external table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


--drop external table playpen.jm_fixedfees_bookings_ext
--CREATE EXTERNAL TABLE playpen.jm_fixedfees_bookings_ext
--(
--	advertiser nvarchar(500)
--	,brands nvarchar(500)
--	,booking nvarchar(4000)
--	,IONumber nvarchar(500)
--	,cost nvarchar(500)
--	,link_screengrab nvarchar(500)
--	,monthsRun nvarchar(500)
--	,EOCReport nvarchar(500)
--	,pageviews nvarchar(500)
--	,clicks nvarchar(500)
--	,sales nvarchar(500)
--)
--WITH (DATA_SOURCE = accurankerIngest,LOCATION = N'FixedFees/bookings',FILE_FORMAT = [myradiotimesonboardingmetadata],REJECT_TYPE = VALUE,REJECT_VALUE = 1)


--------------------------------------------------------------------------------------------------------------------------------------
-- Step 4 - put the new bookings data into the internal table
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG_STEP @Batch_No     = @Batch_No_PV,	
                               @Step_Name    = 'Step 4 - put the new bookings data into the internal table',
                               @Step_Key     = NULL,
                               @Step_Value   = NULL;


truncate table playpen.jm_fixedfees_bookings
insert into playpen.jm_fixedfees_bookings
select * from playpen.jm_fixedfees_bookings_ext


--------------------------------------------------------------------------------------------------------------------------------------
-- Log Complete
--------------------------------------------------------------------------------------------------------------------------------------

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'COMPLETED',
                          @Error_Detail   = NULL;

END TRY

--------------------------------------------------------------------------------------------------------------------------------------
-- Capture Errors
--------------------------------------------------------------------------------------------------------------------------------------

BEGIN CATCH

SELECT @Error_Detail_PV = ERROR_MESSAGE();

EXECUTE MONITOR.P_JOB_LOG @Batch_No       = @Batch_No_PV,		
                          @Proc_Name      = @Proc_Name_PV,		
                          @Proc_Call      = @Proc_Call_PV,		
                          @Exe_Status     = 'ERROR',
                          @Error_Detail   = @Error_Detail_PV;

THROW

END CATCH
END

--------------------------------------------------------------------------------------------------------------------------------------
-- End of Procedure
--------------------------------------------------------------------------------------------------------------------------------------

GO