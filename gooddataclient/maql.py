from gooddataclient.text import to_identifier, to_title
from gooddataclient.columns import Attribute, Fact, Date, Reference, Label, \
    ConnectionPoint

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
        if column.folder:
            if isinstance(column, (Attribute, Label, ConnectionPoint, Reference, Date)):
                if (column.folder, column.folder_title) not in attribute_folders:
                    attribute_folders.append((column.folder, column.folder_title))
            if isinstance(column, (Fact, Date)):
                if (column.folder, column.folder_title) not in fact_folders:
                    fact_folders.append((column.folder, column.folder_title))
    if not attribute_folders and not fact_folders:
        return ''

    maql = ['# CREATE THE FOLDERS THAT GROUP ATTRIBUTES AND FACTS']
    for folder, folder_title in attribute_folders:
        maql.append('CREATE FOLDER {dim.%s} VISUAL(TITLE "%s") TYPE ATTRIBUTE;' \
                    % (folder, folder_title))
    maql.append('')
    for folder, folder_title in fact_folders:
        maql.append('CREATE FOLDER {ffld.%s} VISUAL(TITLE "%s") TYPE FACT;' \
                    % (folder, folder_title))
    maql.append('')
    return '\n'.join(maql)

def maql_attributes(schema_name, column_list):
    maql = ['# CREATE ATTRIBUTES.\n# ATTRIBUTES ARE CATEGORIES THAT ARE USED FOR SLICING AND DICING THE NUMBERS (FACTS)']

    for column in column_list:
        if isinstance(column, (Attribute, ConnectionPoint))\
            or (isinstance(column, Date) and not column.schemaReference):
            maql.append(column.get_maql())
    return '\n'.join(maql)

def maql_facts(schema_name, column_list):
    maql = ['# CREATE FACTS\n# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.']
    for column in column_list:
        if isinstance(column, Fact):
            maql.append(column.get_maql())

    return '\n'.join(maql)

def maql_date_facts(schema_name, column_list):
    maql = ['# CREATE DATE FACTS\n# DATES ARE REPRESENTED AS FACTS\n# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS']
    for column in column_list:
        if isinstance(column, Date) and column.schemaReference:
            maql.append(column.get_maql())

    return '\n'.join(maql)

def maql_references(schema_name, column_list):
    maql = ['# CREATE REFERENCES\n# REFERENCES CONNECT THE DATASET TO OTHER DATASETS']
    for column in column_list:
        if isinstance(column, Reference):
            maql.append(column.get_maql())

    return '\n'.join(maql)

def maql_labels(schema_name, column_list):
    maql = []
    default_set = False
    for column in column_list:
        if isinstance(column, Label):
            maql.append(column.get_maql())
            if not default_set:
                maql.append(column.get_maql_default())
                default_set = True

    cp = False
    for column in column_list:
        if isinstance(column, ConnectionPoint):
            cp = True
            maql.append(column.get_original_label_maql())

    # TODO: not sure where this came from in Department example, wild guess only!
    if not cp:
        maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD LABELS {label.%s.%s} VISUAL(TITLE "%s") AS {f_%s.nm_%s};'\
                % (to_identifier(schema_name), to_identifier(schema_name),
                   to_identifier(schema_name), to_identifier(schema_name),
                   to_title(schema_name), to_identifier(schema_name),
                   to_identifier(schema_name)))
    return '\n'.join(maql)

def maql_synchronize(schema_name):
    return """# SYNCHRONIZE THE STORAGE AND DATA LOADING INTERFACES WITH THE NEW LOGICAL MODEL
SYNCHRONIZE {dataset.%s};
""" % to_identifier(schema_name)

def maql_dataset(dataset):
    schema_name = dataset.schema_name
    column_list = dataset.get_columns()
    return '\n'.join((maql_create(schema_name),
                      maql_folders(column_list),
                      maql_attributes(schema_name, column_list),
                      maql_facts(schema_name, column_list),
                      maql_date_facts(schema_name, column_list),
                      maql_references(schema_name, column_list),
                      maql_labels(schema_name, column_list),
                      maql_synchronize(schema_name)))

