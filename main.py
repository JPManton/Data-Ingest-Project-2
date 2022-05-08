
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from functions import write_to_blob, exec_stored_proc
import os

# Set the criteria for connecting to google sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive',
         'https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name("REDACTED", scope)
client = gspread.authorize(creds)

print("Step 1 Done - Imports and global variables")

# -------------------------------------------------------------------------------------------------------
# Get CPAi data

# Get the worksheet name and the int of the sheet we want
spreadsheet = client.open("Fixed Fee Pipeline 2022")
worksheet = spreadsheet.get_worksheet(1)

# Create dataframe from teh data, deleting the headers that came through in row 0
data = pd.DataFrame(worksheet.get_all_values(),
                    columns=["Advertiser", "oldRate", "newRate", "liveDate",
                             "endDate", "direct", "col1", "col2"]).drop(0)

# Drop the last two irrelevant columns from teh dataframe
data.drop(["col1", "col2"], axis=1, inplace=True)

# Get rid of any non ascii characters from the two rate columns
data['oldRate'].str.encode('ascii', 'ignore').str.decode('ascii')
data['newRate'].str.encode('ascii', 'ignore').str.decode('ascii')

# Save dataframe to csv
data.to_csv("./newCPIsforDW.csv", sep="|", index=False)

# Write to blob ready for sql import
write_to_blob(path_to_file= "./newCPIsforDW.csv", blob_folder= "FixedFees/cpa",
              container= "playpen", storage_account= "005")

# delete file from the folder
os.remove("./newCPIsforDW.csv")

print("Step 2 Done - Upload CPAi data to blob")

# -------------------------------------------------------------------------------------------------------
# Get bookings data

# Get the worksheet name and the int of the sheet we want
spreadsheet = client.open("Fixed Fee Pipeline 2022")
worksheet = spreadsheet.get_worksheet(2)

# Create dataframe from teh data, deleting the headers that came through in row 0
data = pd.DataFrame(worksheet.get_all_values(),
                    columns=["Advertiser", "brands", "booking", "IONumber",
                             "cost", "link_screengrab", "monthsRun", "EOCReport",
                             "pageviews", "clicks", "sales"]).drop([0,1])

# Get rid of any non ascii characters from the two rate columns
data['booking'].str.encode('ascii', 'ignore').str.decode('ascii')
data['booking'] = data['booking'].str.replace("\n", ", ")

# Save dataframe to csv
data.to_csv("./newBookingsforDW.csv", sep="|", index=False)

# Write to blob ready for sql import
write_to_blob(path_to_file= "./newBookingsforDW.csv", blob_folder= "FixedFees/bookings",
              container= "playpen", storage_account= "005")

# delete file from the folder
os.remove("./newBookingsforDW.csv")

print("Step 3 Done - Upload bookings data to blob")

# -------------------------------------------------------------------------------------------------------
# Run stored procedure
exec_stored_proc("exec [playpen].[FF_external_to_internal_import]")

print("Step 4 Done - Run stored procedure to bring into the DW end tables")
