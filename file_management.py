import pandas as pd

def read_capacity(CAPACITY_FILE, airport_substitutions=None):
    capacity = pd.read_csv(CAPACITY_FILE, sep=',')

    capacity['deptime'] = pd.to_datetime(capacity['deptime'])
    capacity['arrtime'] = pd.to_datetime(capacity['arrtime'])

    # Map Weekday_Z to day number (0=Mon, 6=Sun)
    day_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
    capacity['day'] = capacity['Weekday_Z'].map(day_map)

    # Calculate total minutes from start of week (Monday 00:00)
    # Dep Time = Day * 1440 + Hour * 60 + Minute
    capacity['dep_time'] = capacity['day'] * 1440 + capacity['deptime'].dt.hour * 60 + capacity['deptime'].dt.minute
    capacity['DD_Z'] = capacity['DD_Z'].fillna(0)
    capacity['arr_time'] = capacity['day'] * 1440 + capacity['arrtime'].dt.hour * 60 + capacity['arrtime'].dt.minute + capacity['DD_Z']*1440

    # Check for negative durations which indicate data errors
    if (capacity['arr_time'] < capacity['dep_time']).any():
        print("Warning: Some flights have arrival time earlier than departure time after processing. Check data.")
        # Consider dropping or fixing these rows

    # --- Column Renaming and Selection ---
    rename_columns = {
        'Net Payload': 'cap_kg',
        'Net Volume': 'cap_m3',
        'Orig': 'ori',
        'Dest': 'des',
        'Flight Number': 'flight_number',
        'A/C': 'aircraft_type'
    }
    capacity = capacity.rename(columns=rename_columns)

    # Define desired columns
    columns = ['flight_number', 'ori', 'des', 'aircraft_type', 'dep_time', 'arr_time', 'day', 'cap_kg', 'cap_m3']
    capacity = capacity[[col for col in columns if col in capacity.columns]]

    # CONVERT SAME AIRPORTS FROM DICTIONARY - same_airports - CONVERT EVERY KEY TO ITS VALUE
    for key, value in airport_substitutions.items():
        capacity.loc[capacity['ori'] == key, 'ori'] = value
        capacity.loc[capacity['des'] == key, 'des'] = value
    capacity['key'] = capacity['ori'] + '/'+ capacity['des'] +'/'+ capacity['dep_time'].astype(str)

    capacity['dep_time'] = capacity['dep_time'].astype(int)
    capacity['arr_time'] = capacity['arr_time'].astype(int)
    capacity['day'] = capacity['day'].astype(int)
    print(f"Capacity data read: {len(capacity)} rows.")
    return capacity


def read_market(MARKET_FILE, airport_substitutions=None):
    market = pd.read_csv(MARKET_FILE, sep=';')

    market = market.rename(columns={'origin': 'ori', 'destination': 'des', 'Market CHW': 'demand', 'Day': 'day'})
    if 'product' in market.columns: market.drop(columns=['product'], inplace=True)
    if 'Market Allin Yield' in market.columns: market.drop(columns=['Market Allin Yield'], inplace=True)

    day_map = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}
    market['day'] = market['day'].map(day_map)

    # convert HH:MM to minutes
    time_minutes = market['time'].str.split(':', expand=True).astype(int).apply(lambda x: x[0] * 60 + x[1], axis=1)
    market['time'] = market['day'] * 1440 + time_minutes

    # SUBSITUTE AIRPORTS FROM same_airports DICTIONERY - CONVERT EVERY KEY TO ITS VALUE
    for key, value in airport_substitutions.items():
        market.loc[market['ori'] == key, 'ori'] = value
        market.loc[market['des'] == key, 'des'] = value

    # Add original key
    market['key'] = market['ori'] + '/' + market['des'] + '/' + market['time'].astype(str)

    # WE CREATED DUPLICATES KEY AS A RESULT OF CONVERTING SAME AIRPORTS
    # MERGE THE DEMAND VALUES FOR THE SAME KEY
    market = market.groupby(['key', 'ori', 'des', 'day', 'time']).agg({'demand': 'sum'}).reset_index()
    # Select relevant columns
    market = market[['ori', 'des', 'demand', 'day', 'time', 'key']].copy()

    # Convert types
    market['time'] = market['time'].astype(int)
    market['day'] = market['day'].astype(int)
    market['demand'] = pd.to_numeric(market['demand'], errors='coerce').fillna(0)
    print(f"Market data read: {len(market)} rows.")
    return market

# def read_capacity():
#     capacity = pd.read_csv('files/capacity.csv', sep=';')

#     # turn STD-HH:MM into dep_time datetime, and START-HH:MM into arr_time datetime ,  datetime64[ns]!!!
#     capacity['dep_time'] = capacity['STD-HH:MM'].str.replace(':', '').astype(int)
#     capacity['arr_time'] = capacity['STA-HH:MM'].str.replace(':', '').astype(int)

#     # convert Weekday_Z into Day which is just a number, so turn Mon into 0, Tue into 1, ..., Sun into 6
#     capacity['day'] = capacity['Weekday_Z'].map({'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6})
#     capacity['date_time'] = capacity['day'].astype(str) + '/' + capacity['dep_time'].astype(str)

#     # delete the columns that are not needed, reorder the columns
#     rename_columns = {'Net Payload': 'cap_kg', 'Net Volume': 'cap_m3', 'Orig':'ori', 'Dest':'des', 'Flight Number':'flight_number', 'A/C':'aircraft_type'}
#     capacity = capacity.rename(columns=rename_columns)
#     columns = ['flight_number','ori','des','aircraft_type' ,'dep_time', 'arr_time', 'day', 'date_time','cap_kg', 'cap_m3', 'key']
#     capacity['key'] = capacity['ori'] + '/'+ capacity['des'] +'/'+ capacity['date_time']
#     capacity = capacity[columns]
#     return capacity

# def read_market():
#     market = pd.read_csv('files/market.csv', sep=';')

#     market = market.rename(columns={'origin':'ori', 'destination':'des', 'Market CHW': 'demand', 'Day':'day', 'Time':'time'})
#     market.drop(columns=['Market Allin Yield','product'], inplace=True)

#     # turn time into just a 4 digit number
#     market['time'] = market['time'].str.replace(':', '').astype(int)
#     market['day'] = market['day'].map({'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6})
#     market['date_time'] = market['day'].astype(str) + '/' + market['time'].astype(str)
#     market['key'] = market['ori'] + '/'+ market['des'] +'/'+ market['date_time']
#     return market
