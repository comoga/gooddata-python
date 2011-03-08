from gooddataclient.dataset import Dataset

class ExampleDataset(Dataset):

    schema_name = 'forex'
    column_list = [{'name': 'id', 'title': 'Id', 'ldmType': 'CONNECTION_POINT', 'dataType': 'IDENTITY'},
                   {'name': 'time', 'title': 'TIME', 'ldmType': 'DATE', 'datetime': 'true', 'folder': 'Forex', 'schemaReference': 'Forex', 'format': 'dd-MM-yyyy HH:mm:ss'},
                   {'name': 'volume', 'title': 'VOLUME', 'ldmType': 'FACT', 'dataType': 'DECIMAL(8,4)', 'folder': 'Forex'},
                   {'name': 'open', 'title': 'OPEN', 'ldmType': 'FACT', 'dataType': 'DECIMAL(8,4)', 'folder': 'Forex'},
                   {'name': 'close', 'title': 'CLOSE', 'ldmType': 'FACT', 'dataType': 'DECIMAL(8,4)', 'folder': 'Forex'},
                   {'name': 'min', 'title': 'MIN', 'ldmType': 'FACT', 'dataType': 'DECIMAL(8,4)', 'folder': 'Forex'},
                   {'name': 'max', 'title': 'MAX', 'ldmType': 'FACT', 'dataType': 'DECIMAL(8,4)', 'folder': 'Forex'},
                   ]

    def data(self):
        return [{'min': '1.0019', 'max': '1.0026', 'volume': '140', 'time_dt': '40485', 'time': '04-11-2010 00:48:01', 'time_tm': '2881', 'close': '1.0022', 'tm_time_id': '2881', 'open': '1.0023', 'id': 'a4aea808c4d9fc2a11771e7087177546'},
                {'min': '1.0017', 'max': '1.0024', 'volume': '182', 'time_dt': '40485', 'time': '04-11-2010 00:49:01', 'time_tm': '2941', 'close': '1.0022', 'tm_time_id': '2941', 'open': '1.0024', 'id': 'f610d2a7e98bf4a2d1d40f3ba391effb'},
                {'min': '1.0018', 'max': '1.0025', 'volume': '198', 'time_dt': '40485', 'time': '04-11-2010 00:50:01', 'time_tm': '3001', 'close': '1.0023', 'tm_time_id': '3001', 'open': '1.0022', 'id': 'a0c81959893ee94b19b8183a638e0ce6'}
                ]


maql = """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.forex} VISUAL(TITLE "forex");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.forex} VISUAL(TITLE "Forex") TYPE ATTRIBUTE;

CREATE FOLDER {ffld.forex} VISUAL(TITLE "Forex") TYPE FACT;

# CREATE ATTRIBUTES.
# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)
CREATE ATTRIBUTE {attr.forex.id} VISUAL(TITLE "Id") AS KEYS {f_forex.id} FULLSET;
ALTER DATASET {dataset.forex} ADD {attr.forex.id};
ALTER DATATYPE {f_forex.nm_id} VARCHAR(32);
# CREATE FACTS
# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.
CREATE FACT {fact.forex.volume} VISUAL(TITLE "VOLUME", FOLDER {ffld.forex}) AS {f_forex.f_volume};
ALTER DATASET {dataset.forex} ADD {fact.forex.volume};
ALTER DATATYPE {f_forex.f_volume} DECIMAL(8,4);
CREATE FACT {fact.forex.open} VISUAL(TITLE "OPEN", FOLDER {ffld.forex}) AS {f_forex.f_open};
ALTER DATASET {dataset.forex} ADD {fact.forex.open};
ALTER DATATYPE {f_forex.f_open} DECIMAL(8,4);
CREATE FACT {fact.forex.close} VISUAL(TITLE "CLOSE", FOLDER {ffld.forex}) AS {f_forex.f_close};
ALTER DATASET {dataset.forex} ADD {fact.forex.close};
ALTER DATATYPE {f_forex.f_close} DECIMAL(8,4);
CREATE FACT {fact.forex.min} VISUAL(TITLE "MIN", FOLDER {ffld.forex}) AS {f_forex.f_min};
ALTER DATASET {dataset.forex} ADD {fact.forex.min};
ALTER DATATYPE {f_forex.f_min} DECIMAL(8,4);
CREATE FACT {fact.forex.max} VISUAL(TITLE "MAX", FOLDER {ffld.forex}) AS {f_forex.f_max};
ALTER DATASET {dataset.forex} ADD {fact.forex.max};
ALTER DATATYPE {f_forex.f_max} DECIMAL(8,4);
# CREATE DATE FACTS
# DATES ARE REPRESENTED AS FACTS
# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS
CREATE FACT {dt.forex.time} VISUAL(TITLE "TIME (Date)", FOLDER {ffld.forex}) AS {f_forex.dt_time};
ALTER DATASET {dataset.forex} ADD {dt.forex.time};

CREATE FACT {tm.dt.forex.time} VISUAL(TITLE "TIME (Time)", FOLDER {ffld.forex}) AS {f_forex.tm_time};
ALTER DATASET {dataset.forex} ADD {tm.dt.forex.time};

# CONNECT THE DATE TO THE DATE DIMENSION
ALTER ATTRIBUTE {forex.date} ADD KEYS {f_forex.dt_time_id};

# CONNECT THE TIME TO THE TIME DIMENSION
ALTER ATTRIBUTE {attr.time.second.of.day.forex} ADD KEYS {f_forex.tm_time_id};

# CREATE REFERENCES
# REFERENCES CONNECT THE DATASET TO OTHER DATASETS
ALTER ATTRIBUTE {attr.forex.id} ADD LABELS {label.forex.id} VISUAL(TITLE "Id") AS {f_forex.nm_id};
# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.forex};
"""

schema_xml = '''
<schema>
  <name>forex</name>
  <columns>
    <column>
      <name>id</name>
      <title>Id</title>
      <ldmType>CONNECTION_POINT</ldmType>
      <dataType>IDENTITY</dataType>
    </column>
    <column>
      <name>time</name>
      <title>TIME</title>
      <ldmType>DATE</ldmType>
      <datetime>true</datetime>
      <folder>Forex</folder>
      <schemaReference>Forex</schemaReference>
      <format>dd-MM-yyyy HH:mm:ss</format>          
    </column>
    <column>
      <name>volume</name>
      <title>VOLUME</title>
      <ldmType>FACT</ldmType>
      <folder>Forex</folder>
      <dataType>DECIMAL(8,4)</dataType>
    </column>
    <column>
      <name>open</name>
      <title>OPEN</title>
      <ldmType>FACT</ldmType>
      <folder>Forex</folder>
      <dataType>DECIMAL(8,4)</dataType>
    </column>
    <column>
      <name>close</name>
      <title>CLOSE</title>
      <ldmType>FACT</ldmType>
      <folder>Forex</folder>
      <dataType>DECIMAL(8,4)</dataType>
    </column>
    <column>
      <name>min</name>
      <title>MIN</title>
      <ldmType>FACT</ldmType>
      <folder>Forex</folder>
      <dataType>DECIMAL(8,4)</dataType>
    </column>
    <column>
      <name>max</name>
      <title>MAX</title>
      <ldmType>FACT</ldmType>
      <folder>Forex</folder>
      <dataType>DECIMAL(8,4)</dataType>        
    </column>
  </columns>
</schema>
'''

date_dimension_maql = """INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "forex", TITLE "Forex");

# THIS IS MAQL SCRIPT THAT GENERATES TIME DIMENSION LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.time.forex} VISUAL(TITLE "Time (Forex)");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.time.forex} VISUAL(TITLE "Time dimension (Forex)") TYPE ATTRIBUTE;

CREATE FOLDER {ffld.time.forex} VISUAL(TITLE "Time dimension (Forex)") TYPE FACT;

# CREATE ATTRIBUTES.

CREATE ATTRIBUTE {attr.time.second.of.day.forex} VISUAL(TITLE "Time (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_second_of_day_forex.id} FULLSET WITH LABELS {label.time.forex} VISUAL(TITLE "Time (hh:mm:ss)") AS {d_time_second_of_day_forex.nm}, {label.time.twelve.forex} VISUAL(TITLE "Time (HH:mm:ss)") AS {d_time_second_of_day_forex.nm_12}, {label.time.second.of.day.forex} VISUAL(TITLE "Second of Day") AS {d_time_second_of_day_forex.nm_sec};
ALTER ATTRIBUTE {attr.time.second.of.day.forex} ORDER BY {label.time.forex} ASC;
ALTER DATASET {dataset.time.forex} ADD {attr.time.second.of.day.forex};

CREATE ATTRIBUTE {attr.time.second.forex} VISUAL(TITLE "Second (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_second_forex.id} FULLSET, {d_time_second_of_day_forex.second_id} WITH LABELS {label.time.second.forex} VISUAL(TITLE "Second") AS {d_time_second_forex.nm};
ALTER DATASET {dataset.time.forex} ADD {attr.time.second.forex};

CREATE ATTRIBUTE {attr.time.minute.of.day.forex} VISUAL(TITLE "Minute of Day (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_minute_of_day_forex.id} FULLSET, {d_time_second_of_day_forex.minute_id} WITH LABELS {label.time.minute.of.day.forex} VISUAL(TITLE "Minute of Day") AS {d_time_minute_of_day_forex.nm};
ALTER DATASET {dataset.time.forex} ADD {attr.time.minute.of.day.forex};

CREATE ATTRIBUTE {attr.time.minute.forex} VISUAL(TITLE "Minute (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_minute_forex.id} FULLSET, {d_time_minute_of_day_forex.minute_id} WITH LABELS {label.time.minute.forex} VISUAL(TITLE "Minute") AS {d_time_minute_forex.nm};
ALTER DATASET {dataset.time.forex} ADD {attr.time.minute.forex};

CREATE ATTRIBUTE {attr.time.hour.of.day.forex} VISUAL(TITLE "Hour (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_hour_of_day_forex.id} FULLSET, {d_time_minute_of_day_forex.hour_id} WITH LABELS {label.time.hour.of.day.forex} VISUAL(TITLE "Hour (0-23)") AS {d_time_hour_of_day_forex.nm}, {label.time.hour.of.day.twelve.forex} VISUAL(TITLE "Hour (1-12)") AS {d_time_hour_of_day_forex.nm_12};
ALTER ATTRIBUTE {attr.time.hour.of.day.forex} ORDER BY {label.time.hour.of.day.forex} ASC;
ALTER DATASET {dataset.time.forex} ADD {attr.time.hour.of.day.forex};

CREATE ATTRIBUTE {attr.time.ampm.forex} VISUAL(TITLE "AM/PM (Forex)", FOLDER {dim.time.forex}) AS KEYS {d_time_ampm_forex.id} FULLSET, {d_time_hour_of_day_forex.ampm_id} WITH LABELS {label.time.ampm.forex} VISUAL(TITLE "AM/PM") AS {d_time_ampm_forex.nm};
ALTER DATASET {dataset.time.forex} ADD {attr.time.ampm.forex};


# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.time.forex};"""



data_csv = '''"id","time","volume","open","close","min","max","time_dt","time_tm","tm_time_id"
"a4aea808c4d9fc2a11771e7087177546","04-11-2010 00:48:01","140","1.0023","1.0022","1.0019","1.0026","40485","2881","2881"
"f610d2a7e98bf4a2d1d40f3ba391effb","04-11-2010 00:49:01","182","1.0024","1.0022","1.0017","1.0024","40485","2941","2941"
"a0c81959893ee94b19b8183a638e0ce6","04-11-2010 00:50:01","198","1.0022","1.0023","1.0018","1.0025","40485","3001","3001"
'''

sli_manifest = {"dataSetSLIManifest": {
  "parts":   [
        {
      "columnName": "id",
      "mode": "FULL",
      "populates": ["label.forex.id"],
      "referenceKey": 1
    },
        {
      "columnName": "time_dt",
      "mode": "FULL",
      "populates": ["dt.forex.time"]
    },
        {
      "columnName": "time_tm",
      "mode": "FULL",
      "populates": ["tm.dt.forex.time"]
    },
        {
      "columnName": "tm_time_id",
      "mode": "FULL",
      "populates": ["label.time.second.of.day.forex"],
      "referenceKey": 1
    },
        {
      "columnName": "time",
      "mode": "FULL",
      "populates": ["forex.date.mdyy"],
      "constraints": {"date": "dd-MM-yyyy HH:mm:ss"},
      "referenceKey": 1
    },
        {
      "columnName": "volume",
      "mode": "FULL",
      "populates": ["fact.forex.volume"]
    },
        {
      "columnName": "open",
      "mode": "FULL",
      "populates": ["fact.forex.open"]
    },
        {
      "columnName": "close",
      "mode": "FULL",
      "populates": ["fact.forex.close"]
    },
        {
      "columnName": "min",
      "mode": "FULL",
      "populates": ["fact.forex.min"]
    },
        {
      "columnName": "max",
      "mode": "FULL",
      "populates": ["fact.forex.max"]
    }
  ],
  "file": "data.csv",
  "dataSet": "dataset.forex",
  "csvParams":   {
    "quoteChar": "\"",
    "escapeChar": "\"",
    "separatorChar": ",",
    "endOfLine": "\n"
  }
}}
