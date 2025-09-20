import mysql.connector
import pandas as pd
import numpy as np
import json
import re
import unidecode
from rapidfuzz import fuzz
import pycountry
from sqlalchemy import create_engine

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="celiaabts2004",
    database="si_ref_dev"
)

conn = create_engine("mysql+mysqlconnector://root:celiaabts2004@localhost/si_ref_dev")

#querrrries
querry_name = "SELECT DATAID, Concat_ws(' ',`FIRST_NAME`,`SECOND_NAME`,`THIRD_NAME`) as FULL_NAME FROM `onu_individual` "
querry_aleas = "SELECT DATAID, GROUP_CONCAT(ALIAS_NAME SEPARATOR ', ') AS aliases FROM onu_individual_alias GROUP BY DATAID;"
og_name = "SELECT `DATAID`,`NAME_ORIGINAL_SCRIPT` FROM `onu_individual`"
dates = """
SELECT 
    DATAID,
    GROUP_CONCAT(TYPE_OF_DATE) AS date_type,
    GROUP_CONCAT(YEAR) AS years,
    GROUP_CONCAT(FROM_YEAR) AS from_years,
    GROUP_CONCAT(TO_YEAR) AS to_years,
    GROUP_CONCAT(I_DATE) AS exact_date
FROM onu_individual_date_of_birth
GROUP BY DATAID;
"""
doc = """
SELECT 
    `DATAID`,
    `TYPE_OF_DOCUMENT` AS doc_type,
    `NUMBER_DOC` AS doc_num
FROM `onu_individual_document`;
"""
place= """
SELECT 
    `DATAID`,
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'city', CITY,
            'state', STATE_PROVINCE,
            'country', COUNTRY
        )
    ) AS place_of_birth
FROM `onu_individual_place_of_birth`
GROUP BY `DATAID`;

"""
nationality = """
SELECT DATAID, GROUP_CONCAT(VALUE SEPARATOR ',') AS nationality
FROM onu_nationality
GROUP BY DATAID;
"""


#reaad sql
fullname = pd.read_sql(querry_name, conn)
aleas = pd.read_sql(querry_aleas, conn)
og = pd.read_sql(og_name, conn)
dates = pd.read_sql(dates, conn)
document = pd.read_sql(doc,conn)
place_of_birth = pd.read_sql(place, conn)
nat = pd.read_sql(nationality, conn)


# dataframe
data= fullname.merge(aleas, on='DATAID', how='left')
data= data.merge(og, on='DATAID', how='left')
data = data.merge(dates, on='DATAID', how='left')
data= data.merge(document, on='DATAID', how='left')
data = data.merge(place_of_birth, on='DATAID', how='left')
data = data.merge(nat, on='DATAID', how='left')
data.rename(columns={'DATAID': 'ids', 'FULL_NAME': 'fullname', 'NAME_ORIGINAL_SCRIPT' :'og_name'}, inplace=True)

#exact dates 
data['exact_date'] = data['exact_date'].str.split(',')
data['exact_date'] = data['exact_date'].apply(lambda liss: [x for x in liss if x != '1900-01-01' ] if isinstance(liss,list) else liss)
data['exact_date'] = data['exact_date'].apply(lambda x: x if (isinstance(x,list) and len(x)!=0 ) else np.nan)

#date between
data['between date'] = data.apply(
    lambda row: str(min(map(int, row['from_years'].split(',')))) + '-' + str(max(map(int, row['to_years'].split(','))))
    if isinstance(row['date_type'], str) and row['date_type'].count('BETWEEN') == 2
    else np.nan,
    axis=1
)
data['date_type'] = data['date_type'].apply(lambda x: 'BETWEEN' if ( isinstance(x,list) and x.count('BETWEEN')== 2) else x)
data = data.drop(columns=['from_years','to_years'], axis=1)
data['date_type'] = data['date_type'].str.replace(r'\b(EXACT)(,\1)+\b', r'\1', regex=True)

#documents cleaning
data['doc_num'] = data['doc_num'].astype(str).str.replace(r'[^0-9]', '', regex=True)
data['doc_num'] = data['doc_num'].replace('', None)
data['doc_num'] = data['doc_num'].astype('Int64')
def normalize_doc_type(doc):
    if not isinstance(doc, str):
        return None
    
    doc = doc.lower().strip()
    
    # Passport variations
    passport_keywords = ["passport", "número de pasaporte"]
    # National ID variations
    id_keywords = ["id", "identification", "national identification number"]
    
    if any(word in doc for word in passport_keywords):
        return "passport"
    elif any(word in doc for word in id_keywords):
        return "id"
    else:
        return None

data['doc_type'] = data['doc_type'].apply(normalize_doc_type)

    
#place of birth
data['place_of_birth'] = data['place_of_birth'].apply(json.loads)

#natioanlity
def nationality(name):
    if isinstance(name, str):
        try:
            pycountry.countries.lookup(name.lower())
            return name.strip()
        except LookupError:
            return None

    if isinstance(name, list):
        res = []
        for n in name:
            if isinstance(n, str):
                try:
                    pycountry.countries.lookup(n.lower())
                    res.append(n)
                except LookupError:
                    continue
        return res if res else None

    else:
        return None
  
data['nationality'] = data['nationality'].apply(nationality)
#cleaning time for names
def name_normlaize(name):
    if pd.isna(name):
        return ''
    name = name.lower().strip()
    name = re.sub(r'[^a-z\s]', '', name)
    name = re.sub(r'\s+', ' ', name) 
    return name
    
data['fullname'] = data['fullname'].apply(name_normlaize)
data['aliases'] = data['aliases'].fillna('').apply(lambda x: [name_normlaize(d) for d in x.split(',')])

#og_name cleaning(arabic)
def normalize_arabic(text):
    if isinstance(text,str)
    #tashkeel
        text = re.sub(r'[\u064B-\u0652]', '', text)
    #tatweel
        text = re.sub(r'ـ', '', text)
    #alef forms
        text = re.sub(r'[إأآا]', 'ا', text)
    # yaa forms
        text = re.sub(r'[يى]', 'ي', text)
    #taa marbuta
        text = re.sub(r'ة', 'ه', text)
    return text

data['og_name'] = data['og_name'].apply(normalize_arabic, axis=1)

#names in one column
def add_names(name):
    names = []
    if pd.notna(name['fullname']):
        names.append(name['fullname'])
    if pd.notna(name['og_name']):
        names.append(name['og_name'])
    if isinstance(name['aliases'],list):
        names.extend(name['aliases'])
    elif isinstance(name['aliases'],str):
        names.append(name['aliases']) 
    return names

data['all_names'] = data.apply(add_names, axis =1)


print(data.sample(5))
print(len(data))
print(data.columns)

data.to_csv("onu.csv", index=False)