import psycopg2
import requests

# Database connection parameters
db_host = "localhost"
db_name = "destination"
db_user = "postgres"
db_password = "root"
db_port = "5432"  # or your specific port

# API parameters
api_key = "pk.f6e2aa2d0a8c4e9f9a2ffc1fe6904c74"
web_service_url = "https://opencellid.org/cell/get"

# Connect to the database
conn = psycopg2.connect(
    host=db_host,
    dbname=db_name,
    user=db_user,
    password=db_password,
    port=db_port
)

# Create a cursor to interact with the database
cursor = conn.cursor()

# Fetch rows from the database where lat_mast and long_mast are NULL
# and cell_plmn, tac_lac, and cell_utran_id are NOT 0
cursor.execute("""
    SELECT id, cell_plmn, tac_lac, cell_utran_id 
    FROM conn 
    WHERE (lat_mast IS NULL OR long_mast IS NULL) 
    AND cell_plmn != 0 
    AND tac_lac != 0 
    AND cell_utran_id != 0
""")
rows = cursor.fetchall()

# Helper function to convert cell_plmn to MCC and MNC
def extract_mcc_mnc(cell_plmn):
    cell_plmn_str = str(cell_plmn)
    if len(cell_plmn_str) >= 5:
        mcc = int(cell_plmn_str[:3])
        mnc = int(cell_plmn_str[3:])
        return mcc, mnc
    return None, None

# Helper function to get the cell tower location
def get_cell_position(mcc, mnc, tac, cell_id, radio="UMTS", format="json"):
    payload = {
        "key": api_key,
        "mcc": mcc,
        "mnc": mnc,
        "lac": tac,
        "cellid": cell_id,
        "radio": radio,
        "format": format
    }
    response = requests.get(web_service_url, params=payload)
    
    if response.status_code == 200:
        data = response.json()
        lat = data.get("lat")
        lon = data.get("lon")
        return lat, lon
    else:
        print(f"Failed to retrieve position for cell ({mcc}, {mnc}, {tac}, {cell_id})")
        return None, None

# Iterate through the rows fetched from the database
for row in rows:
    record_id, cell_plmn, tac_lac, cell_utran_id = row
    
    # Extract MCC and MNC from cell_plmn
    mcc, mnc = extract_mcc_mnc(cell_plmn)
    
    if mcc is None or mnc is None:
        print(f"Invalid cell_plmn for record {record_id}: {cell_plmn}")
        continue
    
    # Use the API to get the lat and long for the cell tower
    lat_mast, long_mast = get_cell_position(mcc, mnc, tac_lac, cell_utran_id)
    
    if lat_mast is not None and long_mast is not None:
        # Update the database with the lat/long values
        cursor.execute(
            """
            UPDATE conn
            SET lat_mast = %s, long_mast = %s
            WHERE id = %s
            """,
            (lat_mast, long_mast, record_id)
        )
        print(f"Updated record {record_id} with lat_mast: {lat_mast}, long_mast: {long_mast}")
    else:
        print(f"Could not update record {record_id}: API did not return coordinates.")

# Commit the changes to the database
conn.commit()

# Close the cursor and the connection
cursor.close()
conn.close()