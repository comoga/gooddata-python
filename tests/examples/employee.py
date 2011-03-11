from gooddataclient.dataset import Dataset
from gooddataclient.columns import ConnectionPoint, Label, Reference

class Employee(Dataset):

    employee = ConnectionPoint(title='Employee', folder='Employee')
    firstname = Label(title='First Name', reference='employee', folder='Employee')
    lastname = Label(title='Last Name', reference='employee', folder='Employee')
    department = Reference(title='Department', reference='department', schemaReference='Department', folder='Employee')

    def data(self):
        return [{'employee': 'e1', 'lastname': 'Nowmer', 'department': 'd1', 'firstname': 'Sheri'},
             {'employee': 'e2', 'lastname': 'Whelply', 'department': 'd1', 'firstname': 'Derrick'},
             {'employee': 'e6', 'lastname': 'Damstra', 'department': 'd2', 'firstname': 'Roberta'},
             {'employee': 'e7', 'lastname': 'Kanagaki', 'department': 'd3', 'firstname': 'Rebecca'},
             {'employee': 'e8', 'lastname': 'Brunner', 'department': 'd11', 'firstname': 'Kim'},
             {'employee': 'e9', 'lastname': 'Blumberg', 'department': 'd11', 'firstname': 'Brenda'},
             {'employee': 'e10', 'lastname': 'Stanz', 'department': 'd5', 'firstname': 'Darren'},
             {'employee': 'e11', 'lastname': 'Murraiin', 'department': 'd11', 'firstname': 'Jonathan'},
             {'employee': 'e12', 'lastname': 'Creek', 'department': 'd11', 'firstname': 'Jewel'},
             {'employee': 'e13', 'lastname': 'Medina', 'department': 'd11', 'firstname': 'Peggy'},
             {'employee': 'e14', 'lastname': 'Rutledge', 'department': 'd11', 'firstname': 'Bryan'},
             {'employee': 'e15', 'lastname': 'Cavestany', 'department': 'd11', 'firstname': 'Walter'},
             {'employee': 'e16', 'lastname': 'Planck', 'department': 'd11', 'firstname': 'Peggy'},
             {'employee': 'e17', 'lastname': 'Marshall', 'department': 'd11', 'firstname': 'Brenda'},
             {'employee': 'e18', 'lastname': 'Wolter', 'department': 'd11', 'firstname': 'Daniel'},
             {'employee': 'e19', 'lastname': 'Collins', 'department': 'd11', 'firstname': 'Dianne'}
             ]


maql = """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.employee} VISUAL(TITLE "Employee");

# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS
CREATE FOLDER {dim.employee} VISUAL(TITLE "Employee") TYPE ATTRIBUTE;


# CREATE ATTRIBUTES.
# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)
CREATE ATTRIBUTE {attr.employee.employee} VISUAL(TITLE "Employee", FOLDER {dim.employee}) AS KEYS {f_employee.id} FULLSET;
ALTER DATASET {dataset.employee} ADD {attr.employee.employee};

# CREATE FACTS
# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.
# CREATE DATE FACTS
# DATES ARE REPRESENTED AS FACTS
# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS
# CREATE REFERENCES
# REFERENCES CONNECT THE DATASET TO OTHER DATASETS
# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION
ALTER ATTRIBUTE {attr.department.department} ADD KEYS {f_employee.department_id};

# ADD LABELS TO ATTRIBUTES
ALTER ATTRIBUTE {attr.employee.employee} ADD LABELS {label.employee.employee.firstname} VISUAL(TITLE "First Name") AS {f_employee.nm_firstname};

ALTER ATTRIBUTE  {attr.employee.employee} DEFAULT LABEL {label.employee.employee.firstname};
# ADD LABELS TO ATTRIBUTES
ALTER ATTRIBUTE {attr.employee.employee} ADD LABELS {label.employee.employee.lastname} VISUAL(TITLE "Last Name") AS {f_employee.nm_lastname};

ALTER ATTRIBUTE {attr.employee.employee} ADD LABELS {label.employee.employee} VISUAL(TITLE "Employee") AS {f_employee.nm_employee};
# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.employee};
"""

schema_xml = '''
<schema>
  <name>Employee</name>
  <columns>
    <column>
      <name>employee</name>
      <title>Employee</title>
      <ldmType>CONNECTION_POINT</ldmType>
      <folder>Employee</folder>
    </column>
    <column>
      <name>firstname</name>
      <title>First Name</title>
      <ldmType>LABEL</ldmType>
      <reference>employee</reference>
      <folder>Employee</folder>
    </column>
    <column>
      <name>lastname</name>
      <title>Last Name</title>
      <ldmType>LABEL</ldmType>
      <reference>employee</reference>
      <folder>Employee</folder>
    </column>
    <column>
      <name>department</name>
      <title>Department</title>
      <ldmType>REFERENCE</ldmType>
      <reference>department</reference>
      <schemaReference>Department</schemaReference>
      <folder>Employee</folder>
    </column>
  </columns>
</schema>
'''

data_csv = '''"employee","firstname","lastname","department"
"e1","Sheri","Nowmer","d1"
"e2","Derrick","Whelply","d1"
"e6","Roberta","Damstra","d2"
"e7","Rebecca","Kanagaki","d3"
"e8","Kim","Brunner","d11"
"e9","Brenda","Blumberg","d11"
"e10","Darren","Stanz","d5"
"e11","Jonathan","Murraiin","d11"
"e12","Jewel","Creek","d11"
"e13","Peggy","Medina","d11"
"e14","Bryan","Rutledge","d11"
"e15","Walter","Cavestany","d11"
"e16","Peggy","Planck","d11"
"e17","Brenda","Marshall","d11"
"e18","Daniel","Wolter","d11"
"e19","Dianne","Collins","d11"
'''

sli_manifest = {"dataSetSLIManifest": {
  "parts":   [
        {
      "columnName": "employee",
      "mode": "FULL",
      "populates": ["label.employee.employee"],
      "referenceKey": 1
    },
        {
      "columnName": "firstname",
      "mode": "FULL",
      "populates": ["label.employee.employee.firstname"]
    },
        {
      "columnName": "lastname",
      "mode": "FULL",
      "populates": ["label.employee.employee.lastname"]
    },
        {
      "columnName": "department",
      "mode": "FULL",
      "populates": ["label.department.department"],
      "referenceKey": 1
    }
  ],
  "file": "data.csv",
  "dataSet": "dataset.employee",
  "csvParams":   {
    "quoteChar": "\"",
    "escapeChar": "\"",
    "separatorChar": ",",
    "endOfLine": "\n"
  }
}}
