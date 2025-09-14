import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

#Parse the XML file
tree = ET.parse('xmlll.xml')
root = tree.getroot()

# connect to mysql
conn = mysql.connector.connect(
    host='localhost',
    user='root', #personal
    password='celiaabts2004', #personal
    database='si_ref_dev' #database name
)
cursor = conn.cursor()

def parse_date(text):
    """Convert XML string date to MySQL DATE format"""
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except:
        return None
    
    
def insert_table(query, data):
    """Insert data into table with exception handling"""
    try:
        cursor.execute(query, data)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()
        
def insert_individual(ind):
    
    data = {
    "DATAID": ind.findtext("DATAID"),
    "VERSIONNUM": ind.findtext("VERSIONNUM") or 0,  # default to 0 if missing
    "FIRST_NAME": ind.findtext("FIRST_NAME"),
    "SECOND_NAME": ind.findtext("SECOND_NAME"),
    "THIRD_NAME": ind.findtext("THIRD_NAME"),
    "UN_LIST_TYPE": ind.findtext("UN_LIST_TYPE"),
    "REFERENCE_NUMBER": ind.findtext("REFERENCE_NUMBER"),
    "LISTED_ON": parse_date(ind.findtext("LISTED_ON")),
    "NAME_ORIGINAL_SCRIPT": ind.findtext("NAME_ORIGINAL_SCRIPT"),
    "COMMENTS1": ind.findtext("COMMENTS1"),
    "DATE_GENERATED": parse_date(ind.findtext("DATE_GENERATED")),
    "TYPE": ind.findtext("TYPE")
}

    
    query = """
        INSERT INTO ONU_INDIVIDUAL
        (DATAID, VERSIONNUM, FIRST_NAME, SECOND_NAME, THIRD_NAME, UN_LIST_TYPE,
         REFERENCE_NUMBER, LISTED_ON, NAME_ORIGINAL_SCRIPT, COMMENTS1, DATE_GENERATED, TYPE)
        VALUES (%(DATAID)s, %(VERSIONNUM)s, %(FIRST_NAME)s, %(SECOND_NAME)s, %(THIRD_NAME)s, %(UN_LIST_TYPE)s,
                %(REFERENCE_NUMBER)s, %(LISTED_ON)s, %(NAME_ORIGINAL_SCRIPT)s, %(COMMENTS1)s, %(DATE_GENERATED)s, %(TYPE)s)
        ON DUPLICATE KEY UPDATE
            FIRST_NAME = VALUES(FIRST_NAME),
            SECOND_NAME = VALUES(SECOND_NAME)
    """
    
    insert_table(query, data)

def insert_address(ind):
    for addr in ind.findall("INDIVIDUAL_ADDRESS"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "CITY": addr.findtext("CITY") or "",
            "COUNTRY": addr.findtext("COUNTRY") or "",
            "STATE_PROVINCE": addr.findtext("STATE_PROVINCE") or "",
            "NOTE": addr.findtext("NOTE") or "",
            "STREET": addr.findtext("STREET") or ""
        }
        query = """
            INSERT INTO ONU_INDIVIDUAL_ADDRESS
            (DATAID, CITY, COUNTRY, STATE_PROVINCE, NOTE, STREET)
            VALUES (%(DATAID)s, %(CITY)s, %(COUNTRY)s, %(STATE_PROVINCE)s, %(NOTE)s, %(STREET)s)
            ON DUPLICATE KEY UPDATE
                NOTE = VALUES(NOTE)
        """

        insert_table(query, data)
    
def insert_alias(ind):
    for alias in ind.findall("INDIVIDUAL_ALIAS"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "QUALITY": alias.findtext("QUALITY") or "",
            "ALIAS_NAME": alias.findtext("ALIAS_NAME") or "",
            "CITY_OF_BIRTH": alias.findtext("CITY_OF_BIRTH") or "",
            "NOTE": alias.findtext("NOTE") or "",
            "COUNTRY_OF_BIRTH": alias.findtext("COUNTRY_OF_BIRTH") or ""
        }
        query = """
            INSERT INTO ONU_INDIVIDUAL_ALIAS
            (DATAID, QUALITY, ALIAS_NAME, CITY_OF_BIRTH, NOTE, COUNTRY_OF_BIRTH)
            VALUES (%(DATAID)s, %(QUALITY)s, %(ALIAS_NAME)s, %(CITY_OF_BIRTH)s, %(NOTE)s, %(COUNTRY_OF_BIRTH)s)
            ON DUPLICATE KEY UPDATE
                QUALITY = VALUES(QUALITY)
        """
        insert_table(query, data)
        
def insert_date_of_birth(ind):
    for dob in ind.findall("INDIVIDUAL_DATE_OF_BIRTH"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "TYPE_OF_DATE": dob.findtext("TYPE_OF_DATE") or "",
            "YEAR": dob.findtext("YEAR") or "",
            "I_DATE": parse_date(dob.findtext("I_DATE")) or datetime(1900,1,1).date(),
            "FROM_YEAR": dob.findtext("FROM_YEAR") or "",
            "TO_YEAR": dob.findtext("TO_YEAR") or "",
            "NOTE": dob.findtext("NOTE") or ""
        }
        query = """
            INSERT INTO ONU_INDIVIDUAL_DATE_OF_BIRTH
            (DATAID, TYPE_OF_DATE, YEAR, I_DATE, FROM_YEAR, TO_YEAR, NOTE)
            VALUES (%(DATAID)s, %(TYPE_OF_DATE)s, %(YEAR)s, %(I_DATE)s, %(FROM_YEAR)s, %(TO_YEAR)s, %(NOTE)s)
            ON DUPLICATE KEY UPDATE
                NOTE = VALUES(NOTE)
        """
        insert_table(query, data)

  
def insert_document(ind):
    for doc in ind.findall("INDIVIDUAL_DOCUMENT"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "TYPE_OF_DOCUMENT": doc.findtext("TYPE_OF_DOCUMENT") or "",
            "NUMBER_DOC": doc.findtext("NUMBER") or "",
            "ISSUING_COUNTRY": doc.findtext("ISSUING_COUNTRY") or "",
            "NOTE": doc.findtext("NOTE") or "",
            "TYPE_OF_DOCUMENT2": doc.findtext("TYPE_OF_DOCUMENT2") or ""
        }
        query = """
            INSERT INTO ONU_INDIVIDUAL_DOCUMENT
            (DATAID, TYPE_OF_DOCUMENT, NUMBER_DOC, ISSUING_COUNTRY, NOTE, TYPE_OF_DOCUMENT2)
            VALUES (%(DATAID)s, %(TYPE_OF_DOCUMENT)s, %(NUMBER_DOC)s, %(ISSUING_COUNTRY)s, %(NOTE)s, %(TYPE_OF_DOCUMENT2)s)
            ON DUPLICATE KEY UPDATE
                NOTE = VALUES(NOTE)
        """
        insert_table(query, data)    
        
def insert_place_of_birth(ind):
    for pob in ind.findall("INDIVIDUAL_PLACE_OF_BIRTH"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "CITY": pob.findtext("CITY") or "",
            "STATE_PROVINCE": pob.findtext("STATE_PROVINCE") or "",
            "COUNTRY": pob.findtext("COUNTRY") or ""
        }
        query = """
            INSERT INTO ONU_INDIVIDUAL_PLACE_OF_BIRTH
            (DATAID, CITY, STATE_PROVINCE, COUNTRY)
            VALUES (%(DATAID)s, %(CITY)s, %(STATE_PROVINCE)s, %(COUNTRY)s)
            ON DUPLICATE KEY UPDATE
                CITY = VALUES(CITY)
        """
        insert_table(query, data)  

def insert_last_day_updated(ind):
    value = ind.findtext("LAST_DAY_UPDATED")
    if value:
        data = {
            "DATAID": ind.findtext("DATAID"),
            "VALUE": value
        }
        query = """
            INSERT INTO ONU_LAST_DAY_UPDATED
            (DATAID, VALUE)
            VALUES (%(DATAID)s, %(VALUE)s)
            ON DUPLICATE KEY UPDATE
                VALUE = VALUES(VALUE)
        """
        insert_table(query, data)
def insert_nationality(ind):
    for nat in ind.findall("NATIONALITY"):
        # Look for VALUE sub-element within NATIONALITY
        value_elem = nat.find("VALUE")
        if value_elem is None or not value_elem.text or not value_elem.text.strip():
            continue
            
        data = {
            "DATAID": ind.findtext("DATAID"),
            "VALUE": value_elem.text.strip()
        }
        query = """
            INSERT INTO ONU_NATIONALITY (DATAID, VALUE)
            VALUES (%(DATAID)s, %(VALUE)s)
            ON DUPLICATE KEY UPDATE
                VALUE = VALUES(VALUE)
        """
        print("///////////////////////hgj")
        print(data)
        insert_table(query, data)

        
def insert_designation(ind):
    for des in ind.findall("DESIGNATION"):
        data = {
            "DATAID": ind.findtext("DATAID"),
            "VALUE": des.text or ""
        }
        query = """
            INSERT INTO ONU_DESIGNATION
            (DATAID, VALUE)
            VALUES (%(DATAID)s, %(VALUE)s)
            ON DUPLICATE KEY UPDATE
                VALUE = VALUES(VALUE)
        """
        insert_table(query, data)
        
for ind in root.findall(".//INDIVIDUAL"):
    insert_individual(ind)
    insert_address(ind)
    insert_alias(ind)
    insert_date_of_birth(ind)
    insert_document(ind)
    insert_place_of_birth(ind)
    insert_last_day_updated(ind)
    insert_nationality(ind)
    insert_designation(ind)

        
cursor.close()
conn.close()

print("XML data successfully imported into MySQL!")