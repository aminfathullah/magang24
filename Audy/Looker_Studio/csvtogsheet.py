import pandas as pd
import pygsheets

def upload_to_google_sheets(df, sheet_name, worksheet_index):
    try:
        # Authorize pygsheets with the JSON file
        gc = pygsheets.authorize(service_account_file="D:\\Coding\\magang-423902-f0e642da9b61.json")
        
        # Open Google Sheet
        gs = gc.open(sheet_name)
        worksheet = gs.worksheet('index', worksheet_index)
        
        # Clear the worksheet and update with new data
        worksheet.clear('A1')
        
        worksheet.set_dataframe(df, 'A1', extend=True, copy_index=True)
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")

def main():
    # Read data from file (e.g., CSV)
    file_path = 'D:\\Coding\\Python\\magang24\\audy\\crawling\\Output_Crawling_Harga\\datafinal.csv'
    df = pd.read_csv(file_path)
    
    # Upload to Google Sheets
    upload_to_google_sheets(df, 'Data harga', 0)

if __name__ == "__main__":
    main()
