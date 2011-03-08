from gooddataclient.text import to_identifier, to_title
from gooddataclient.columns import Attribute, Fact, Date, Reference, Label

def maql_create(schema_name):
    return """
# THIS IS MAQL SCRIPT THAT GENERATES PROJECT LOGICAL MODEL.
# SEE THE MAQL DOCUMENTATION AT http://developer.gooddata.com/api/maql-ddl.html FOR MORE DETAILS

# CREATE DATASET. DATASET GROUPS ALL FOLLOWING LOGICAL MODEL ELEMENTS TOGETHER.
CREATE DATASET {dataset.%s} VISUAL(TITLE "%s");
""" % (to_identifier(schema_name), to_title(schema_name))

def maql_folders(column_list):
    attribute_folders, fact_folders = [], []
    for column in column_list:
        if 'folder' in column:
            if column['ldmType'] in ('ATTRIBUTE', 'LABEL', 'CONNECTION_POINT',
                                     'REFERENCE', 'DATE'):
                if column['folder'] not in attribute_folders:
                    attribute_folders.append(column['folder'])
            if column['ldmType'] in ('FACT', 'DATE'):
                if column['folder'] not in fact_folders:
                    fact_folders.append(column['folder'])
    if not attribute_folders and not fact_folders:
        return ''

    maql = ['# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS']
    for folder in attribute_folders:
        maql.append('CREATE FOLDER {dim.%s} VISUAL(TITLE "%s") TYPE ATTRIBUTE;' \
                    % (to_identifier(folder), to_title(folder)))
    maql.append('')
    for folder in fact_folders:
        maql.append('CREATE FOLDER {ffld.%s} VISUAL(TITLE "%s") TYPE FACT;' \
                    % (to_identifier(folder), to_title(folder)))
    maql.append('')
    return '\n'.join(maql)

def maql_attributes(schema_name, column_list):
    maql = ['# CREATE ATTRIBUTES.\n# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)']

    for column in column_list:
        if column['ldmType'] in ('ATTRIBUTE', 'CONNECTION_POINT')\
            or (column['ldmType'] == 'DATE' and 'schemaReference' not in column):
            maql.append(Attribute(schema_name=schema_name, name=column['name'], title=column['title'],
                                  folder=column['folder'] if 'folder' in column else None,
                                  dataType=column['dataType'] if 'dataType' in column else None).get_maql())
    return '\n'.join(maql)

def maql_facts(schema_name, column_list):
    maql = ['# CREATE FACTS\n# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.']
    for column in column_list:
        if column['ldmType'] == 'FACT':
            maql.append(Fact(schema_name=schema_name, name=column['name'], title=column['title'],
                                  folder=column['folder'] if 'folder' in column else None,
                                  dataType=column['dataType'] if 'dataType' in column else None).get_maql())
            
    return '\n'.join(maql)

def maql_date_facts(schema_name, column_list):
    maql = ['# CREATE DATE FACTS\n# DATES ARE REPRESENTED AS FACTS\n# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS']
    for column in column_list:
        if column['ldmType'] == 'DATE' and 'schemaReference' in column:
            maql.append(Date(schema_name=schema_name, name=column['name'], title=column['title'],
                                  folder=column['folder'] if 'folder' in column else None,
                                  dataType=column['dataType'] if 'dataType' in column else None,
                                  schemaReference=column['schemaReference'] if 'schemaReference' in column else None,
                                  datetime=('datetime' in column)).get_maql())

    return '\n'.join(maql)

def maql_references(schema_name, column_list):
    maql = ['# CREATE REFERENCES\n# REFERENCES CONNECT THE DATASET TO OTHER DATASETS']
    for column in column_list:
        if column['ldmType'] == 'REFERENCE':
            maql.append(Reference(schema_name=schema_name, name=column['name'], title=column['title'],
                                  folder=column['folder'] if 'folder' in column else None,
                                  dataType=column['dataType'] if 'dataType' in column else None,
                                  schemaReference=column['schemaReference'] if 'schemaReference' in column else None,
                                  reference=column['reference'] if 'reference' in column else None,
                                  datetime=('datetime' in column)).get_maql())

    return '\n'.join(maql)

def maql_labels(schema_name, column_list):
    maql = []
    default_set = False
    for column in column_list:
        if column['ldmType'] == 'LABEL':
            label = Label(schema_name=schema_name, name=column['name'], title=column['title'],
                                  folder=column['folder'] if 'folder' in column else None,
                                  dataType=column['dataType'] if 'dataType' in column else None,
                                  schemaReference=column['schemaReference'] if 'schemaReference' in column else None,
                                  reference=column['reference'] if 'reference' in column else None,
                                  datetime=('datetime' in column))
            maql.append(label.get_maql())
            if not default_set:
                maql.append(label.get_maql_default())
                default_set = True

    cp = None
    for column in column_list:
        if column['ldmType'] == 'CONNECTION_POINT':
            cp = column

    maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD LABELS {label.%s.%s} VISUAL(TITLE "%s") AS {f_%s.nm_%s};'\
                % (to_identifier(schema_name), to_identifier(cp['name'] if cp else schema_name),
                   to_identifier(schema_name), to_identifier(cp['name'] if cp else schema_name),
                   to_title(cp['title'] if cp else schema_name), to_identifier(schema_name),
                   to_identifier(cp['name'] if cp else schema_name)))
    return '\n'.join(maql)

def maql_synchronize(schema_name):
    return """# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.%s};
""" % to_identifier(schema_name)

def maql_dataset(schema_name, column_list):
    return '\n'.join((maql_create(schema_name),
                      maql_folders(column_list),
                      maql_attributes(schema_name, column_list),
                      maql_facts(schema_name, column_list),
                      maql_date_facts(schema_name, column_list),
                      maql_references(schema_name, column_list),
                      maql_labels(schema_name, column_list),
                      maql_synchronize(schema_name)))

