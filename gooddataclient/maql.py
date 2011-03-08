from gooddataclient.text import to_identifier, to_title

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
            try:
                folder_statement = ', FOLDER {dim.%s}' % to_identifier(column['folder'])
            except KeyError:
                folder_statement = ''
            fks = ''
            maql.append('CREATE ATTRIBUTE {attr.%s.%s} VISUAL(TITLE "%s"%s) AS KEYS {f_%s.id} FULLSET%s;'\
                        % (to_identifier(schema_name), to_identifier(column['name']),
                           to_title(column['title']), folder_statement, to_identifier(schema_name),
                           fks))
            maql.append('ALTER DATASET {dataset.%s} ADD {attr.%s.%s};'\
                        % (to_identifier(schema_name), to_identifier(schema_name), 
                           to_identifier(column['name'])))

            if 'dataType' in column:
                data_type = 'VARCHAR(32)' if column['dataType'] == 'IDENTITY' else column['dataType']
                maql.append('ALTER DATATYPE {f_%s.nm_%s} %s;' \
                            % (to_identifier(schema_name), to_identifier(column['name']),
                               data_type))
            else:
                maql.append('')

    return '\n'.join(maql)

def maql_facts(schema_name, column_list):
    maql = ['# CREATE FACTS\n# FACTS ARE NUMBERS THAT ARE AGGREGATED BY ATTRIBUTES.']
    for column in column_list:
        if column['ldmType'] == 'FACT':
            try:
                folder_statement = ', FOLDER {ffld.%s}' % to_identifier(column['folder'])
            except KeyError:
                folder_statement = ''
            maql.append('CREATE FACT {fact.%s.%s} VISUAL(TITLE "%s"%s) AS {f_%s.f_%s};'\
                        % (to_identifier(schema_name), to_identifier(column['name']),
                           to_title(column['title']), folder_statement,
                           to_identifier(schema_name), to_identifier(column['name'])))
            maql.append('ALTER DATASET {dataset.%s} ADD {fact.%s.%s};'\
                        % (to_identifier(schema_name), to_identifier(schema_name),
                           to_identifier(column['name'])))

            if 'dataType' in column:
                data_type = 'VARCHAR(32)' if column['dataType'] == 'IDENTITY' else column['dataType']
                maql.append('ALTER DATATYPE {f_%s.f_%s} %s;' \
                            % (to_identifier(schema_name), to_identifier(column['name']),
                               data_type))
            else:
                maql.append('')

    return '\n'.join(maql)

def maql_date_facts(schema_name, column_list):
    maql = ['# CREATE DATE FACTS\n# DATES ARE REPRESENTED AS FACTS\n# DATES ARE ALSO CONNECTED TO THE DATE DIMENSIONS']
    for column in column_list:
        if column['ldmType'] == 'DATE' and 'schemaReference' in column:
            try:
                folder_statement = ', FOLDER {ffld.%s}' % to_identifier(column['folder'])
            except KeyError:
                folder_statement = ''
            maql.append('CREATE FACT {dt.%s.%s} VISUAL(TITLE "%s (Date)"%s) AS {f_%s.dt_%s};'\
                        % (to_identifier(schema_name), to_identifier(column['name']),
                           to_title(column['title']), folder_statement,
                           to_identifier(schema_name), to_identifier(column['name'])))
            maql.append('ALTER DATASET {dataset.%s} ADD {dt.%s.%s};\n'\
                        % (to_identifier(schema_name), to_identifier(schema_name),
                           to_identifier(column['name'])))
            if 'datetime' in column:
                maql.append('CREATE FACT {tm.dt.%s.%s} VISUAL(TITLE "%s (Time)"%s) AS {f_%s.tm_%s};'\
                            % (to_identifier(schema_name), to_identifier(column['name']),
                               to_title(column['title']), folder_statement,
                               to_identifier(schema_name), to_identifier(column['name'])))
                maql.append('ALTER DATASET {dataset.%s} ADD {tm.dt.%s.%s};\n'\
                            % (to_identifier(schema_name), to_identifier(schema_name),
                               to_identifier(column['name'])))

            maql.append('# CONNECT THE DATE TO THE DATE DIMENSION')
            maql.append('ALTER ATTRIBUTE {%s.date} ADD KEYS {f_%s.dt_%s_id};\n'\
                        % (to_identifier(column['schemaReference']),
                           to_identifier(schema_name), to_identifier(column['name'])))
            if 'datetime' in column:
                maql.append('# CONNECT THE TIME TO THE TIME DIMENSION')
                maql.append('ALTER ATTRIBUTE {attr.time.second.of.day.%s} ADD KEYS {f_%s.tm_%s_id};\n'\
                            % (to_identifier(column['schemaReference']),
                               to_identifier(schema_name), to_identifier(column['name'])))
                

    return '\n'.join(maql)

def maql_references(schema_name, column_list):
    maql = ['# CREATE REFERENCES\n# REFERENCES CONNECT THE DATASET TO OTHER DATASETS']
    for column in column_list:
        if column['ldmType'] == 'REFERENCE':
            maql.append('# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION')
            maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD KEYS {f_%s.%s_id};\n'\
                        % (to_identifier(column['schemaReference']),
                           to_identifier(column['reference']),
                           to_identifier(schema_name),
                           to_identifier(column['name'])))

    return '\n'.join(maql)

def maql_labels(schema_name, column_list):
    maql = []
    default_set = False
    for column in column_list:
        if column['ldmType'] == 'LABEL':
            maql.append('# ADD LABELS TO ATTRIBUTES')
            maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD LABELS {label.%s.%s.%s} VISUAL(TITLE "%s") AS {f_%s.nm_%s};' \
                        % (to_identifier(schema_name), to_identifier(column['reference']),
                           to_identifier(schema_name), to_identifier(column['reference']),
                           to_identifier(column['name']), to_title(column['title']),
                           to_identifier(schema_name), to_identifier(column['name'])))
            # TODO: DATATYPE
            maql.append('')
            if not default_set:
                maql.append('ALTER ATTRIBUTE  {attr.%s.%s} DEFAULT LABEL {label.%s.%s.%s};'\
                            % (to_identifier(schema_name), to_identifier(column['reference']),
                               to_identifier(schema_name), to_identifier(column['reference']),
                               to_identifier(column['name'])))
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

