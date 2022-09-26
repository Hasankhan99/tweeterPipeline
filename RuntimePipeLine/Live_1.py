from __future__ import print_function

# Builtin Modules in Python

import requests
import json
from multiprocessing import Process, Queue
import time
import re
import datetime as dt
import sys
# Theses Modules Should Be Installed First

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from finbert_v1.main import finbert_predict
from sentence_tr.check_similarity import CheckSimilarity
from topic_model.topic_model import topicModel
from googleapiclient.errors import HttpError
import re
import string

# ----------------------------------------------------------------


# Google DriveAPI Class All Drive Functions are implemented in this Class.
class DriveAPI:
    global SCOPES

    # Define the scopes
    SCOPES = [
        'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets']

    # SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self):
        KEY_FILE_LOCATION = 'Service_Account_Credentials.json'
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
            self.addSheets([dt.date.today().strftime("%m-%d-%Y")], id)
            COLUMN_NAMES = [["Time_Date_(US_EST_Time)", "Author_ID", "Raw_Tweets", "Processed_Tweets", "Similarity_Result",
                             "Topic_Result", "Finbert_Result", "Stock_Ticker", "Tweet_id", "CompanyNames_Keywords"]]
            self.deleteSheets(["Sheet1"], id)
            self.append_rows_in_sheet(
                id, dt.date.today().strftime("%m-%d-%Y"), COLUMN_NAMES)
        except HttpError as e:
            print(e)

    def addSheets(self, sheet_names, id):
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

    def deleteSheets(self, sheet_names, id):
        spreadsheet_body = {
            "requests": [
            ]
        }
        for sheet_name in sheet_names:
            spreadsheet_body['requests'].append({
                "deleteSheet": {
                    "sheetId": self.get_sheet_id(id, sheet_name)
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

    def append_rows_in_sheet(self, file_id, sheet_name, values):
        """Appends rows in a sheet"""
        request = self.sheet.spreadsheets().values().append(
            spreadsheetId=file_id, range=sheet_name, valueInputOption='USER_ENTERED', body={
                'values': values
            }).execute()
        print("Appended ", " successfully!")
        return request

    def appendSheet(self, spreadsheet_id, sheet_name, values):
        range_ = sheet_name
        value_input_option = 'RAW'
        value_range_body = {'values': values}
        request = self.sheet.spreadsheets().values().append(spreadsheetId=spreadsheet_id,
                                                            range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()
        print(response)


bearer_token = 'AAAAAAAAAAAAAAAAAAAAAIyagQEAAAAAPcWOadw1ynrzGfyoiBaOvJTWe80%3DnG5ZuXhNKDcQHwENDat1AY3bWwMfqFuO29JCuKNbr8mjXf1pIQ'
data = pd.read_csv('Clean_data_final_v2.csv')
KEYWORD = pd.read_csv("keyword.csv")['Keyword'].values.tolist()
Comp_name_ticker = pd.read_csv('Comp_name_&_ticker.csv')


def matching_ticker(company):
    ticker = list()
    for i in company:
        ticker.append(
            Comp_name_ticker.loc[Comp_name_ticker['Company'] == i, 'Ticker'].values[0])
    return ticker


def matching_company(text, companies):
    l = list()
    for i in companies:
        if i.lower() in text:
            l.append(i)
    return l


def matching_keyword(text):
    l = list()
    for i in KEYWORD:
        if i.lower() in text:
            l.append(i)
    return l


def get_companies_from_tag_dict():
    dic = dict()
    for i in data['Tag'].unique():
        l = list()
        for j in range(len(data)):
            if data.loc[j, 'Tag'] == i:
                l.append(data.loc[j, 'Company_name'])
        dic[str(i)] = l
    return dic


TAG_DICT = get_companies_from_tag_dict()



def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                        "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)
def clean_tweet(text):
    strange_symbol='''Â™€£¥ααβγδεζηθικλμνξπρστυφχψωΩאגדהוזחטיכלמנס9פצקרשת?+ּ♣☞÷:–<>∅∞∂⊗∑∅βδεζηθικλμνξπρστυφχψäëïöüÿÄËÏÖÜŸáćéíńóśúýźÁĆÉÍŃÓŚÚÝŹőűŐŰà`è`ì`ò`ùÀÈ`Ì`Ò`Ù`â^ê^^ô^û^Â^Ê^^Ô^Û^ã~ñ~õ~Ã~Ñ~Õ~čďěǧgňřšťžČĎĚǦGŇŘŠŤŽ<đĐęĄĘ;æÆøØç,Ç,łŁßþżŻ¡¿?€£¥ααβγδεζηθικλμνξπρστυφχψωΩאגדהוזחטיכלמנס9פצקרשת?+ּ♣☞÷:–<>∅∞∂⊗∑∅βδεζηθικλμνξπρστυφχψ'''
    symbol=(string.punctuation.replace('$', ''))
    symbol=str(symbol)+str(strange_symbol)
    text = re.sub('['+symbol+']', '', text)
    text = re.sub(r'http\S+', '', text)
    text= re.sub(' +', ' ',text)
    # text = re.sub(r'[]', ' ', text)
    text=re.sub(r'\s(?=\s)', '', text)
    text=deEmojify(text)

    return text


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )


def set_rules(delete):
    # Please use capital "OR" and don't contain any 'and' in the rules.
    sample_rules = [
        {
            "value": "(BlackRock Inc. OR S&P Global Inc. OR Intuit Inc. OR Cisco Systems Inc. OR Abbott Laboratories OR Walt Disney Company OR Walmart Inc. OR Bank of America Corp OR NVIDIA Corporation) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "1"},
        {
            "value": "(Microsoft Corporation OR Visa Inc OR Tesla Inc OR Amazon Inc OR Nasdaq, Inc. OR Nike Inc OR PepsiCo OR Procter Gamble OR Target Corporation OR Texas Instruments) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "2"},
        {
            "value": "(Coca-Cola Company OR Home Depot Inc. OR Twitter Inc. OR Yum! Brands OR Wynn Resorts OR Union Pacific Corporation OR United Airlines Holdings OR UnitedHealth Group OR Tyson Foods)(acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "3"},
        {
            "value": "(A. O. Smith OR AbbVie OR Abiomed OR AES Corporation OR Agilent OR Air Products Chemicals OR Akamai OR Albemarle OR Align Technology OR Allegion OR Alliant Energy) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "4"},
        {
            "value": "(Alphabet Inc OR Amcor OR American Electric Power OR American Express OR American International Group OR American Tower OR American Water Works OR Ameriprise Financial) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "5"},
        {
            "value": "(AmerisourceBergen OR Ametek OR Amgen OR Amphenol OR Analog Devices OR APA Corporation OR Apple Inc OR Applied Materials OR Aptiv OR Arista Networks OR Arthur J. Gallagher Co.) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "6"},
        {
            "value": "(Atmos Energy OR Autodesk OR AvalonBay Communities OR Avery Dennison OR Baker Hughes OR Ball Corporation OR Bath Body Works OR Baxter International OR Becton Dickinson) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "7"},
        {
            "value": "(Berkshire Hathaway OR Biogen OR Bio-Rad OR Bio-Techne OR BNY Mellon OR Booking Holdings OR BorgWarner OR Boston Scientific OR Bristol Myers Squibb OR Broadcom Inc.) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "8"},
        {
            "value": "(Broadridge Financial OR Brown Brown OR Brown–Forman OR C.H. Robinson OR Cadence Design Systems OR Caesars Entertainment OR Camden Property Trust OR Campbell Soup Company) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "9"},
        {
            "value": "(Cardinal Health OR Carrier Global OR Catalent OR Caterpillar Inc. OR Cboe Global Markets OR CBRE Group OR Celanese OR Centene Corporation OR CenterPoint Energy OR Ceridian) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "10"},
        {
            "value": "(CF Industries OR Charles River Laboratories OR Charles Schwab OR Charter Communications OR Chevron OR Chipotle Mexican Grill OR Chubb Limited OR Church Dwight OR Cigna) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "11"},
        {
            "value": "(Cincinnati Financial OR Citigroup OR Citizens Financial OR Citrix OR Clorox OR CME Group OR CMS Energy OR Cognizant OR Colgate-Palmolive OR Comerica OR Conagra Brands) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "12"},
        {
            "value": "(ConocoPhillips OR Consolidated Edison OR Constellation Brands OR Constellation Energy OR CooperCompanies OR Copart OR Corning Inc. OR Corteva OR CoStar Group OR Coterra) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "13"},
        {
            "value": "(Crown Castle OR Cummins OR CVS Health OR D.R. Horton OR Danaher OR Darden Restaurants OR DaVita Inc. OR Dentsply Sirona OR Devon Energy OR Dexcom OR Diamondback Energy) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "14"},
        {
            "value": "(Digital Realty OR Discover Financial OR Dominion Energy OR Dover Corporation OR Dow Inc. OR DTE Energy OR Duke Energy OR Duke Realty OR DuPont OR DXC Technology) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "15"},
        {
            "value": "(Eastman Chemical Company OR Eaton Corporation OR Ecolab OR Edison International OR Edwards Lifesciences OR Elevance Health OR Eli Lilly Company OR Emerson Electric) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "16"},
        {
            "value": "(EOG Resources OR EPAM Systems OR Essex Property Trust OR Everest Re OR Exelon OR Expedia Group OR Expeditors International OR Extra Space Storage OR ExxonMobil OR F5, Inc.) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "17"},
        {
            "value": "(FactSet OR Fastenal OR Federal Realty OR Fifth Third Bank OR First Republic Bank OR FirstEnergy OR Fiserv OR Fleetcor OR FMC Corporation OR Ford Motor Company OR Fortinet) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "18"},
        {
            "value": "(Fortive OR Fortune Brands Home Security OR Fox Corporation OR Franklin Templeton OR Freeport-McMoRan OR Garmin OR Gartner OR Generac OR General Dynamics OR General Electric) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "19"},
        {
            "value": "(General Mills OR General Motors OR Genuine Parts Company OR Gilead Sciences OR Globe Life OR Goldman Sachs OR Halliburton OR HCA Healthcare OR Healthpeak OR Henry Schein) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "20"},
        {
            "value": "(Hess Corporation OR Hewlett Packard Enterprise OR Hilton Worldwide OR Hologic OR Honeywell OR Hormel Foods OR Host Hotels Resorts OR Howmet Aerospace OR HP Inc.) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "21"},
        {
            "value": "(Huntington Bancshares OR Huntington Ingalls Industries OR IDEX Corporation OR Idexx Laboratories OR Illinois Tool Works OR Ingersoll Rand OR Intercontinental Exchange) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "22"},
        {
            "value": "(International Flavors Fragrances OR Intuit OR Intuitive Surgical OR Iron Mountain OR J.B. Hunt OR Jack Henry Associates OR Jacobs Solutions OR Johnson Johnson) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "23"},
        {
            "value": "(Johnson Controls OR JPMorgan Chase OR Juniper Networks OR Keurig Dr Pepper OR KeyCorp OR Keysight OR Kimberly-Clark OR Kimco Realty OR Kinder Morgan OR KLA Corporation) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "24"},
        {
            "value": "(Kraft Heinz OR L3Harris OR LabCorp OR Lam Research OR Lamb Weston OR Las Vegas Sands OR Lennar OR Lincoln Financial OR Linde plc OR LKQ Corporation OR Lockheed Martin) (acquire OR deal OR spinoff OR shareholders OR board OR split OR merge OR vote OR buy OR sell OR product OR Launch OR license OR agreement OR lay off OR resign OR insider OR executive OR ceo OR hiring OR Breach OR hackers OR criminal OR misconduct OR investigation OR legal OR fda OR Analyst OR shares OR activist) lang:en",
            "tag": "25"}
    ]
# Loews Corporation OR Lumen Technologies OR LyondellBasell OR Marathon Oil OR Marathon Petroleum OR MarketAxess OR
    # Marriott International OR Marsh McLennan OR Martin Marietta Materials OR Masco OR Match Group OR McCormick
    # Company OR Medtronic OR Merck Co. OR Meta Platforms OR Mettler Toledo OR MGM Resorts OR Microchip Technology
    # OR Micron Technology OR Moderna OR Mohawk Industries OR Molina Healthcare OR Molson Coors Beverage OR Mondelez
    # International OR Monolithic Power Systems OR Monster Beverage OR Moody's Corporation OR Morgan Stanley OR
    # Motorola Solutions OR PerkinElmer OR Philip Morris International OR Phillips 66 OR Pinnacle West OR Pioneer
    # Natural Resources OR PNC Financial Services OR Pool Corporation OR PPG Industries OR PPL Corporation OR
    # Principal Financial Group OR Progressive Corporation OR Prologis OR Prudential Financial OR Public Service
    # Enterprise Group OR PulteGroup OR Qorvo OR Qualcomm OR Quanta Services OR Quest Diagnostics OR Ralph Lauren
    # Corporation OR Raymond James OR Regency Centers OR Regeneron OR Regions Financial Corporation OR Republic
    # Services OR ResMed OR Robert Half OR Rockwell Automation OR Roper Technologies OR Royal Caribbean Group OR S&P
    # Global OR SBA Communications OR Schlumberger OR Seagate Technology OR Sealed Air OR Security OR Sempra Energy
    # OR ServiceNow OR Sherwin-Williams OR Signature Bank OR Simon Property Group OR Skyworks Solutions OR SolarEdge
    # OR Southern Company OR Stanley Black Decker OR State Street Corporation OR Stryker Corporation OR SVB
    # Financial OR Synchrony Financial OR Synopsys OR Sysco OR T. Rowe Price OR Take-Two Interactive OR Tapestry,
    # Inc. OR TE Connectivity OR Teledyne Technologies OR Teradyne OR The Interpublic Group of Companies OR The J.M.
    # Smucker Company OR The Mosaic Company OR The Travelers Companies OR Thermo Fisher Scientific OR TJX Companies
    # OR Trane Technologies OR TransDigm Group OR Trimble Inc. OR Truist OR Tyler Technologies OR UDR,
    # Inc. OR Universal Health Services OR Valero Energy OR Verisign OR Verisk OR Vertex Pharmaceuticals OR VF
    # Corporation OR Viatris OR Vici Properties OR Vornado Realty Trust OR Vulcan Materials Company OR W. W. Grainger
    # OR Wabtec OR Walgreens Boots Alliance OR Warner Bros. Discovery OR Waters Corporation OR WEC Energy Group OR
    # Welltower OR West Pharmaceutical Services OR WestRock OR Weyerhaeuser OR Whirlpool Corporation OR Williams
    # Companies OR Willis Towers Watson OR Xcel Energy OR Xylem Inc. OR Zebra Technologies OR Zimmer Biomet OR Zions
    # OR Zoetis

    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(
                response.status_code, response.text)
        )


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
        params={
            'tweet.fields': ['author_id,created_at,text']
        }
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    return response


def Producer(buffer):
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    response = get_stream(set)
    print("in Producer")
    start = time.time()
    PERIOD_OF_TIME = 28800
    for response_line in response.iter_lines():
        if time.time() > start + PERIOD_OF_TIME:
            print("Producer Exit")
            sys.exit()
        try:
            if response_line:
                json_response = json.loads(response_line)
                buffer.put(json_response)
        except:
            pass


def Consumer(buffer, post_buffer):
    topic = topicModel()
    similarity = CheckSimilarity()
    start = time.time()
    PERIOD_OF_TIME = 28800
    while True:
        if buffer.empty() and time.time() > start + PERIOD_OF_TIME:
            print("Consumer exit")
            sys.exit()
        try:
            if not buffer.empty():
                item = buffer.get()
                # print(item['data']['text'])
                message = clean_tweet(item['data']['text'])
                if (len(message) > 10):
                    # print(message)
                    company_names = matching_company(
                        message, TAG_DICT[item['matching_rules'][0]['tag']])
                    if company_names != []:
                        sim = similarity.check_similarity(message)
                        # if float(sim[0]) >= 0.5:
                        topic_result = topic.predictTopic(message)
                        # if any(t > 0.5 for t in topic_result.values()):
                        finbert_result = finbert_predict(message)
                        # print(sim, topic_result, finbert_result)
                        comp_key_dict = {
                            'company_names': company_names, 'keywords': matching_keyword(message)}
                        # ["Time_Date_(US_EST_Time)","Author_ID","Raw_Tweets","Processed_Tweets","Similarity_Result","Topic_Result","Finbert_Result","Stock_Ticker","Tweet_id","CompanyNames_Keywords"]
                        post_buffer.put([item['data']['created_at'], item['data']['author_id'], item['data']['text'], message, json.dumps([float(sim[0]), sim[1]]), json.dumps(
                            topic_result), finbert_result, json.dumps(matching_ticker(company_names)), item['data']['id'], json.dumps(comp_key_dict)])
        except:
            pass


def Post_data(post_buffer):
    drive = DriveAPI()
    file_list = drive.get_file_list_in_folder()
    sheet_name = dt.date.today().strftime("%m-%d-%Y")
    Current_Month = dt.date.today().strftime("%Y-%m")
    if Current_Month in file_list.keys():
        time.sleep(1)
        spreadsheet_id = file_list[dt.date.today().strftime("%Y-%m")]
        time.sleep(1)
        if sheet_name not in drive.get_sheet_names(spreadsheet_id):
            time.sleep(1)
            drive.addSheets([sheet_name], spreadsheet_id)
            time.sleep(1)
            drive.append_rows_in_sheet(spreadsheet_id, sheet_name, [["Time_Date_(US_EST_Time)", "Author_ID", "Raw_Tweets", "Processed_Tweets",
                                       "Similarity_Result", "Topic_Result", "Finbert_Result", "Stock_Ticker", "Tweet_id", "CompanyNames_Keywords"]])
            print("New Sheet Added")
        else:
            print("Sheet already exists")
    else:
        spreadsheet_id = drive.create_new_spreadsheet_with_parentID(
            Current_Month)
        print("New Spreadsheet Created")
    start = time.time()
    PERIOD_OF_TIME = 28800
    while True:
        if post_buffer.empty() and time.time() > start + PERIOD_OF_TIME:
            print("Post Data exit")
            sys.exit()
        values = []
        time.sleep(1)
        while not post_buffer.empty():
            values.append(post_buffer.get())
        try:
            if values != []:
                drive.appendSheet(spreadsheet_id, sheet_name, values)
                values = []
        except:
            pass


def main():
    buffer = Queue()
    post_buffer = Queue()
    p = Process(target=Producer, args=(buffer,))
    c = Process(target=Consumer, args=(buffer, post_buffer))
    post = Process(target=Post_data, args=(post_buffer,))
    c.start()
    p.start()
    post.start()
    p.join()
    c.join()
    post.join()


if __name__ == "__main__":
    main()
