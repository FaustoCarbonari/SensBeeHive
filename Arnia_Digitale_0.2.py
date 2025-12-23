import pandas as pd
from datetime import datetime, timedelta
from Google_Sheets_Service import Create_Service

# Autenticazione e autorizzazione alla Google Sheets API
CLIENT_SECRET_FILE = 'Token_Arnia_0.2.json'
API_NAME = 'sheets'
API_VERSION ='v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Crea i servizi drive e sheets
sheets_service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

# Definisci l'ID del file di origine e di destinazione
source_file_id = '1htjbCiCb05NMjnSEX0wS8YKLkomcrnAypvly6phqfkI'
destination_file_id = '1WNH1bZCfzB5P-nq7QbN2Z1sBTBIt1DUK7KN1Yg2uG4E'
sheet_id = '0'

# Scarica i dati dal foglio specificato nel file di origine
sheet_data = sheets_service.spreadsheets().values().get(spreadsheetId = source_file_id, 
                                                        range = 'A:Z',
                                                        majorDimension = 'ROWS', 
                                                        valueRenderOption = 'UNFORMATTED_VALUE', 
                                                        dateTimeRenderOption = 'FORMATTED_STRING').execute()
values = sheet_data.get('values', [])


# Processa i dati
if values:
    # Estrai i dati dalla lista di liste
    data = [item[0] for item in values]

    # Divide i dati in nuove colonne
    split_data = [item.split(';') for item in data]
        
    # Modifica la prima riga di intestazione
    column_names = ['Date', 'Time', 'Weight(Kg)', 'BroodT(°C)', 'NestT(°C)', 'NestRH(%)', 'Roof', 'Bottom']  # Roof e Bottom (fotoresistori) 0 chiuso; 1 aperto
    split_data[0] = column_names

    # Modifica la prima colonna escludendo l'intestazione
    for row in split_data[1:]:
        if len(row) >= 1 and len(row[0]) >= 8:
            row[0] = row[0][:4] + '/' + row[0][4:6] + '/' + row[0][6:8]

    # Modifica la seconda colonna escludendo l'intestazione
    for row in split_data[1:]:
        if len(row) >= 2 and len(row[1]) >= 3:
            row[1] = row[1][:2] + ':' + row[1][2:]

    # Crea un DataFrame con le nuove colonne
    df = pd.DataFrame(split_data)

    # Converti in formato numerico le colonne dei sensori
    columns_to_convert = df.columns[2:]
    df.loc[1:, columns_to_convert]=df.loc[1:, columns_to_convert].astype(float)
    
    # Inserisci i dati nel foglio di destinazione nel file di destinazione
    sheets_service.spreadsheets().values().update(
        spreadsheetId=destination_file_id,
        range='A1',  # Inizia dalla cella A1
        body={'values': df.values.tolist()},  # Inserisci l'intestazione prima dei dati
        valueInputOption='USER_ENTERED',
    ).execute()
    print('Dati inseriti con successo nel file di destinazione.')


# Scarica i dati dal foglio con i dati trasformati
sheet_data_processed = sheets_service.spreadsheets().values().get(spreadsheetId = destination_file_id, 
                                                            range = 'A:Z',
                                                            majorDimension = 'ROWS', 
                                                            valueRenderOption = 'UNFORMATTED_VALUE', 
                                                            dateTimeRenderOption = 'FORMATTED_STRING').execute()
processed_values = sheet_data_processed.get('values', [])


# Ottieni la data odierna, della settimana scorsa e del mese scorso formattata come 'aaaa/mm/gg'
today_date = datetime.now().strftime('%Y/%m/%d')

last_week_date = (datetime.now() - timedelta(days=7)).strftime('%Y/%m/%d')
last_week_date_1 = (datetime.now() - timedelta(days=6)).strftime('%Y/%m/%d')

first_day_last_month = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y/%m/%d')
last_day_last_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y/%m/%d')


# Ottieni start_row_index e end_row_index per i dati odierni
today_start_row_index = None
today_end_row_index = None

for i, row in enumerate(processed_values, start=0):
    if row and row[0] == today_date:
        if today_start_row_index is None:
            today_start_row_index = i
        today_end_row_index = i + 1


# Ottieni start_row_index e end_row_index per gli ultimi 7 giorni
week_start_row_index = None
week_end_row_index = None

for i, row in enumerate(processed_values, start=1):
    if row and row[0] == last_week_date:
        week_start_row_index = i
    elif row and row[0] == today_date:
        week_end_row_index = i


# Ottieni l'intervallo delle righe per il mese scorso
month_start_row_index = None
month_end_row_index = None

for i, row in enumerate(processed_values, start=1):
    if row and first_day_last_month <= row[0] <= last_day_last_month:
        if month_start_row_index is None:
            month_start_row_index = i
        month_end_row_index = i

try:
    # Prima, esegui una richiesta per ottenere i metadati dei fogli nel tuo documento
    sheets_metadata = sheets_service.spreadsheets().get(spreadsheetId=destination_file_id).execute()

    # Estrai l'elenco dei fogli
    sheets_list = sheets_metadata.get('sheets', [])

    # Inizializza una lista vuota per le richieste
    requests_to_delete = []

    # Cicla attraverso tutti i fogli (a partire dal secondo) e mantieni solo il primo foglio
    for sheet in sheets_list[1:]:
        sheet_id1 = sheet['properties']['sheetId']

        # Crea una richiesta per eliminare il foglio
        delete_request = {
            'deleteSheet': {
                'sheetId': sheet_id1
            }
        }

        # Aggiungi la richiesta di eliminazione alla lista delle richieste
        requests_to_delete.append(delete_request)

    if requests_to_delete:
        # Esegui una richiesta di aggiornamento per eliminare i fogli selezionati
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=destination_file_id, 
            body={'requests': requests_to_delete}
        ).execute()
        print('Fogli eliminati con successo.')
    else:
        print('Nessun altro foglio da eliminare.')

except Exception as e:
    print(f'Errore durante l\'eliminazione dei fogli: {str(e)}')

# Definisci la funzione per creare i grafici combinati
def create_combo_chart(sheets_service, spreadsheet_id, chart_title, x_axis_title, y1_axis_title, y2_axis_title, header_row, data_range, y_column, y1_column, y2_column, label_x_column):
    request_body = {
        'requests': [
            {
                'addChart': {
                    'chart': {
                        'spec': {
                            'title': chart_title,
                            'basicChart': {
                                'chartType': 'COMBO',
                                'legendPosition': 'BOTTOM_LEGEND',
                                'headerCount': header_row,
                                'axis': [
                                    {
                                        'position': "BOTTOM_AXIS",
                                        'title': x_axis_title
                                    },
                                    {
                                        'position': "LEFT_AXIS",
                                        'title': y1_axis_title
                                    },
                                    {
                                        'position': "RIGHT_AXIS",
                                        'title': y2_axis_title
                                    }
                                ],
                                'series': [
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [
                                                    {
                                                        'sheetId': sheet_id,
                                                        'startRowIndex': data_range[0],
                                                        'endRowIndex': data_range[1],
                                                        'startColumnIndex': y_column[0],
                                                        'endColumnIndex': y_column[1]
                                                    }
                                                ]
                                            }
                                        },
                                        'targetAxis': 'LEFT_AXIS',
                                        'type': 'COLUMN',
                                    },
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [
                                                    {
                                                        'sheetId': sheet_id,
                                                        'startRowIndex': data_range[0],
                                                        'endRowIndex': data_range[1],
                                                        'startColumnIndex': y1_column[0],
                                                        'endColumnIndex': y1_column[1]
                                                    }
                                                ]
                                            }
                                        },
                                        'targetAxis': 'RIGHT_AXIS',
                                        'type': 'LINE'
                                    },
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [
                                                    {
                                                        'sheetId': sheet_id,
                                                        'startRowIndex': data_range[0],
                                                        'endRowIndex': data_range[1],
                                                        'startColumnIndex': y2_column[0],
                                                        'endColumnIndex': y2_column[1]
                                                    }
                                                ]
                                            }
                                        },
                                        'targetAxis': 'RIGHT_AXIS',
                                        'type': 'LINE'
                                    }
                                ],
                                'domains': [
                                    {
                                        'domain': {
                                            'sourceRange': {
                                                'sources': [
                                                    {
                                                        'sheetId': sheet_id,
                                                        'startRowIndex': data_range[0],
                                                        'endRowIndex': data_range[1],
                                                        'startColumnIndex': label_x_column[0],
                                                        'endColumnIndex': label_x_column[1]
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        },
                        'position': {
                            'newSheet': True
                        }
                    }
                }
            }
        ]
    }
    response = sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=request_body).execute()

# Crea i grafici combinati
all_data_chart = create_combo_chart(sheets_service, destination_file_id, 
                                    "All data Temperature and Weight", "Date", "Weight(Kg)", "Temperature(°C)", 
                                    (1), (0, today_end_row_index), (2, 3), (3, 4), (4, 5), (0, 1))

month_data_chart = create_combo_chart(sheets_service, destination_file_id, 
                                      "Month data Temperature and Weight", f'Month ({first_day_last_month} - {last_day_last_month})', "Weight(Kg)", "Temperature(°C)", 
                                      (None), (month_start_row_index, month_end_row_index), (2, 3), (3, 4), (4, 5), (0, 1))

week_data_chart = create_combo_chart(sheets_service, destination_file_id, 
                                      "Week data Temperature and Weight", f'Week ({last_week_date_1} - {today_date})', "Weight(Kg)", "Temperature(°C)", 
                                      (None), (week_start_row_index, week_end_row_index), (2, 3), (3, 4), (4, 5), (0, 1))

today_data_chart = create_combo_chart(sheets_service, destination_file_id, 
                                      "Today data Temperature and Weight", f'Date ({today_date})', "Weight(Kg)", "Temperature(°C)", 
                                      (None), (today_start_row_index, today_end_row_index), (2, 3), (3, 4), (4, 5), (1, 2))
