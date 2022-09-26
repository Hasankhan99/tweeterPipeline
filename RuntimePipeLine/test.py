from __future__ import print_function
from tkinter.tix import COLUMN
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
import datetime as dt
class DriveAPI:
    global SCOPES

    # Define the scopes
    SCOPES = [
        'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets']

    # SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self):
        KEY_FILE_LOCATION = 'D:\kwantx\Twitter_Project\models\RuntimePipeLine\Service_Account_Credentials.json'
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)
      
        # Connect to the API drive
        self.drive = build('drive', 'v3', credentials=self.creds)
        self.sheet = build('sheets', 'v4', credentials=self.creds)
      

    def get_file_list_in_folder(self, topFolderId='1Nw1x2hetLl4Pqjs55BC_JaNk3rEjutQX'):
        try:
            self.empty_trash()
            pageToken = ""
            items = []
            while pageToken is not None:
                response = self.drive.files().list(q="'" + topFolderId + "' in parents", pageSize=1000,
                                                   pageToken=pageToken, fields="nextPageToken, files(id, name)").execute()
                items.extend(response.get('files', []))
                pageToken = response.get('nextPageToken')
                # print(items)
            name_id_dicts = {}
            for i in items:
                name_id_dicts[i['name']] = i['id']
            return name_id_dicts
        except HttpError:
            # Stop if there is an HttpError
            raise

    def create_folder_in_parent_folder(self, folder_name, parent_folder_id='1Nw1x2hetLl4Pqjs55BC_JaNk3rEjutQX'):
        """ Create a folder and prints the folder ID
        Returns : Folder Id
        """
        try:
            file_metadata = {
                'title': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }

            file = self.drive.files().create(body=file_metadata, fields='id'
                                             ).execute()
            print(F'Folder has created with ID: "{file.get("id")}".')

        except HttpError as error:
            print(F'An error occurred: {error}')
            file = None

        return file.get('id')

    def create_new_spreadsheet_with_parentID(self, spread_sheet_title, parent_id='1Nw1x2hetLl4Pqjs55BC_JaNk3rEjutQX'):
        try:
            file_metadata = {
                'name': spread_sheet_title, 'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [parent_id]}
            request = self.drive.files().create(
                body=file_metadata)
            response = request.execute()
            id = response['id']
            self.addSheets([dt.date.today().strftime("%m-%d-%Y")],id)
            COLUMN_NAMES = [["Time_Date_(US_EST_Time)","Author_ID","Raw_Tweets","Processed_Tweets","Similarity_Result","Topic_Result","Finbert_Result","Stock_Ticker","Tweet_id","Company_names_&_Keywords"]]
            self.deleteSheets(["Sheet1"],id)
            self.append_rows_in_sheet(id,dt.date.today().strftime("%m-%d-%Y"),COLUMN_NAMES)
        except HttpError as e:
            print(e)

    def addSheets(self, sheet_names,id):
        spreadsheet_body = {
                "requests": [
                ]
            }
        for sheet_name in sheet_names:
            spreadsheet_body['requests'].append({
                "addSheet": {"properties": {"title": str(sheet_name)}}
            })

        request = self.sheet.spreadsheets().batchUpdate(
            spreadsheetId=id, body=spreadsheet_body)
        response = request.execute()
        return response

    def deleteSheets(self,sheet_names,id):
        spreadsheet_body = {
            "requests": [
            ]
        }
        for sheet_name in sheet_names:
            spreadsheet_body['requests'].append({
                "deleteSheet": {
                    "sheetId": self.get_sheet_id(id,sheet_name)
                }})

        request = self.sheet.spreadsheets().batchUpdate(
            spreadsheetId=id, body=spreadsheet_body)
        response = request.execute()
        return response


    def empty_trash(self):
        """Empty trash"""
        request = self.drive.files().list(q="trashed = true").execute()
        for file in request['files']:
            self.drive.files().delete(fileId=file['id']).execute()

    def delete_spreadsheet(self, spreadsheet_id):
        """Deletes a spreadsheet"""
        request = self.sheet.spreadsheets().delete(
            spreadsheetId=spreadsheet_id)
        response = request.execute()
        return response

    def get_sheet_id(self, spreadsheet_id, sheet_name):
        """Returns the sheet ID of a sheet with a given name"""
        request = self.sheet.spreadsheets().get(
            spreadsheetId=spreadsheet_id)
        response = request.execute()
        for sheet in response['sheets']:
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        return None

    def get_sheet_names(self, spreadsheet_id):
        """Returns the sheet ID of a sheet with a given name"""
        request = self.sheet.spreadsheets().get(
            spreadsheetId=spreadsheet_id)
        response = request.execute()
        list = [sheet['properties']['title'] for sheet in response['sheets']]
        return list


    def drive_file_delete(self, file_id):
        """Deletes a file"""
        request = self.drive.files().delete(fileId=file_id)
        response = request.execute()
        print("Deleted file: ", response)
        # return response


    def append_rows_in_sheet(self,file_id,sheet_name, values):
        """Appends rows in a sheet"""
        request = self.sheet.spreadsheets().values().append(
            spreadsheetId=file_id, range=sheet_name, valueInputOption='USER_ENTERED', body={
                'values': values
            }).execute()
        print("Appended "," successfully!")
        return request
    


    def appendSheet(self,spreadsheet_id,sheet_name,values):
        range_ = sheet_name  
        value_input_option = 'RAW'
        value_range_body = {'values': values}
        request = self.sheet.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()
        print(response)


if __name__ == "__main__":
    obj = DriveAPI()
    obj.create_new_spreadsheet_with_parentID('2022-09')

    pass