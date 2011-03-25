from gooddataclient.text import to_identifier, to_title


class Column(object):

    ldmType = None
    referenceKey = False

    def __init__(self, title, folder=None, reference=None,
                 schemaReference=None, dataType=None, datetime=False, format=None):
        self.title = to_title(title)
        self.folder = to_identifier(folder)
        self.folder_title = to_title(folder)
        self.reference = to_identifier(reference)
        self.schemaReference = to_identifier(schemaReference)
        self.dataType = dataType
        self.datetime = datetime
        self.format = format

    def get_schema_values(self):
        values = []
        for key in ('name', 'title', 'folder', 'reference', 'schemaReference',
                    'dataType', 'datetime', 'format'):
            value = getattr(self, key)
            if value:
                if isinstance(value, bool):
                    value = 'true'
                values.append((key, value))
        return values

    def get_sli_manifest_part(self):
        part = {"columnName": self.name,
                "mode": "FULL",
                }
        if self.referenceKey:
            part["referenceKey"] = 1
        if self.format:
            part['constraints'] = {'date': self.format}
        try:
            part['populates'] = self.populates()
        except NotImplementedError:
            pass
        return part

    def populates(self):
        raise NotImplementedError


class Attribute(Column):

    ldmType = 'ATTRIBUTE'
    referenceKey = True

    def get_maql(self):
        folder_statement = ''
        if self.folder:
            folder_statement = ', FOLDER {dim.%s}' % self.folder
        fks = ''
        maql = []
        maql.append('CREATE ATTRIBUTE {attr.%s.%s} VISUAL(TITLE "%s"%s) AS KEYS {f_%s.id} FULLSET%s;'\
                    % (self.schema_name, self.name, self.title, folder_statement,
                       self.schema_name, fks))
        maql.append('ALTER DATASET {dataset.%s} ADD {attr.%s.%s};'\
                    % (self.schema_name, self.schema_name, self.name))

        if self.dataType:
            data_type = 'VARCHAR(32)' if self.dataType == 'IDENTITY' else self.dataType
            maql.append('ALTER DATATYPE {f_%s.nm_%s} %s;' \
                        % (self.schema_name, self.name, data_type))
        else:
            maql.append('')
        return '\n'.join(maql)

    def populates(self):
        return ["label.%s.%s" % (self.schema_name, self.name)]

class ConnectionPoint(Attribute):

    ldmType = 'CONNECTION_POINT'

    def get_original_label_maql(self):
        return 'ALTER ATTRIBUTE {attr.%s.%s} ADD LABELS {label.%s.%s} VISUAL(TITLE "%s") AS {f_%s.nm_%s};'\
                % (self.schema_name, self.name, self.schema_name, self.name,
                   self.title, self.schema_name, self.name)


class Fact(Column):

    ldmType = 'FACT'

    def get_maql(self):
        folder_statement = ''
        if self.folder:
            folder_statement = ', FOLDER {ffld.%s}' % self.folder
        maql = []
        maql.append('CREATE FACT {fact.%s.%s} VISUAL(TITLE "%s"%s) AS {f_%s.f_%s};'\
                    % (self.schema_name, self.name, self.title, folder_statement,
                       self.schema_name, self.name))
        maql.append('ALTER DATASET {dataset.%s} ADD {fact.%s.%s};'\
                    % (self.schema_name, self.schema_name, self.name))

        if self.dataType:
            data_type = 'VARCHAR(32)' if self.dataType == 'IDENTITY' else self.dataType
            maql.append('ALTER DATATYPE {f_%s.f_%s} %s;' \
                        % (self.schema_name, self.name, data_type))
        else:
            maql.append('')
        return '\n'.join(maql)

    def populates(self):
        return ["fact.%s.%s" % (self.schema_name, self.name)]

class Date(Column):

    ldmType = 'DATE'
    referenceKey = True

    def get_maql(self):
        folder_statement = ''
        if self.folder:
            folder_statement = ', FOLDER {ffld.%s}' % self.folder
        maql = []
        maql.append('CREATE FACT {dt.%s.%s} VISUAL(TITLE "%s (Date)"%s) AS {f_%s.dt_%s};'\
                    % (self.schema_name, self.name, self.title, folder_statement,
                       self.schema_name, self.name))
        maql.append('ALTER DATASET {dataset.%s} ADD {dt.%s.%s};\n'\
                    % (self.schema_name, self.schema_name, self.name))
        if self.datetime:
            maql.append('CREATE FACT {tm.dt.%s.%s} VISUAL(TITLE "%s (Time)"%s) AS {f_%s.tm_%s};'\
                        % (self.schema_name, self.name, self.title, folder_statement,
                           self.schema_name, self.name))
            maql.append('ALTER DATASET {dataset.%s} ADD {tm.dt.%s.%s};\n'\
                        % (self.schema_name, self.schema_name, self.name))

        maql.append('# CONNECT THE DATE TO THE DATE DIMENSION')
        maql.append('ALTER ATTRIBUTE {%s.date} ADD KEYS {f_%s.dt_%s_id};\n'\
                    % (self.schemaReference, self.schema_name, self.name))
        if self.datetime:
            maql.append('# CONNECT THE TIME TO THE TIME DIMENSION')
            maql.append('ALTER ATTRIBUTE {attr.time.second.of.day.%s} ADD KEYS {f_%s.tm_%s_id};\n'\
                        % (self.schemaReference, self.schema_name, self.name))
        return '\n'.join(maql)

    def populates(self):
        return ["%s.date.mdyy" % self.schemaReference]

class Reference(Column):

    ldmType = 'REFERENCE'
    referenceKey = True

    def get_maql(self):
        maql = []
        maql.append('# CONNECT THE REFERENCE TO THE APPROPRIATE DIMENSION')
        maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD KEYS {f_%s.%s_id};\n'\
                    % (self.schemaReference, self.reference, self.schema_name,
                       self.name))
        return '\n'.join(maql)

    def populates(self):
        return ["label.%s.%s" % (self.schemaReference, self.reference)]


class Label(Column):

    ldmType = 'LABEL'

    def get_maql(self):
        maql = []
        maql.append('# ADD LABELS TO ATTRIBUTES')
        maql.append('ALTER ATTRIBUTE {attr.%s.%s} ADD LABELS {label.%s.%s.%s} VISUAL(TITLE "%s") AS {f_%s.nm_%s};' \
                    % (self.schema_name, self.reference, self.schema_name,
                       self.reference, self.name, self.title, self.schema_name,
                       self.name))
        # TODO: DATATYPE
        maql.append('')
        return '\n'.join(maql)

    def get_maql_default(self):
        return 'ALTER ATTRIBUTE  {attr.%s.%s} DEFAULT LABEL {label.%s.%s.%s};'\
                % (self.schema_name, self.reference, self.schema_name,
                   self.reference, self.name)

    def populates(self):
        return ["label.%s.%s.%s" % (self.schema_name, self.reference, self.name)]


# TODO: create proper columns with utilizing get_sli_manifest_part method
def get_date_dt_column(column, schema_name):
    name = '%s_dt' % column.name
    populates = 'dt.%s.%s' % (to_identifier(schema_name), column.name)
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_time_tm_column(column, schema_name):
    name = '%s_tm' % column.name
    populates = 'tm.dt.%s.%s' % (to_identifier(schema_name), column.name)
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_tm_time_id_column(column, schema_name):
    name = 'tm_%s_id' % column.name
    populates = 'label.time.second.of.day.%s' % column.schemaReference
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL', 'referenceKey': 1}
