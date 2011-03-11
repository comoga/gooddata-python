from gooddataclient import text
from gooddataclient.archiver import CSV_DATA_FILENAME
from gooddataclient.text import to_identifier
from gooddataclient.columns import Attribute, ConnectionPoint, Reference, Date,\
    Label, Fact

# TODO: put this into columns and dataset

def get_column_populates(column, schema_name):
    schema_name_id = text.to_identifier(schema_name)
    if isinstance(column, (Attribute, ConnectionPoint)):
        return ["label.%s.%s" % (schema_name_id, column.name)]
    if isinstance(column, Label):
        return ["label.%s.%s.%s" % (schema_name_id, column.reference, column.name)]
    if isinstance(column, Reference):
        return ["label.%s.%s" % (column.schemaReference, column.reference)]
    if isinstance(column, Fact):
        return ["fact.%s.%s" % (schema_name_id, column.name)]
    if isinstance(column, Date):
        return ["%s.date.mdyy" % column.schemaReference]
    raise AttributeError, 'Nothing to populate'

def get_date_dt_column(column, schema_name):
    name = '%s_dt' % column.name
    populates = 'dt.%s.%s' % (text.to_identifier(schema_name), column.name)
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_time_tm_column(column, schema_name):
    name = '%s_tm' % column.name
    populates = 'tm.dt.%s.%s' % (text.to_identifier(schema_name), column.name)
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_tm_time_id_column(column, schema_name):
    name = 'tm_%s_id' % column.name
    populates = 'label.time.second.of.day.%s' % column.schemaReference
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL', 'referenceKey': 1}

def get_sli_manifest(column_list, schema_name):
    '''Create JSON manifest from columns in schema.
    
    @param column_list: list of dicts (created from XML schema)
    @param schema_name: string
    
    See populateColumnsFromSchema in AbstractConnector.java
    '''
    parts = []
    for column in column_list:
        # special additional column for date
        if isinstance(column, Date):
            parts.append(get_date_dt_column(column, schema_name))
            if column.datetime:
                parts.append(get_time_tm_column(column, schema_name))
                parts.append(get_tm_time_id_column(column, schema_name))


        part = {"columnName": column.name,
                "mode": "FULL",
                }
        if isinstance(column, (Attribute, ConnectionPoint, Reference, Date)):
            part["referenceKey"] = 1
        if column.format:
            part['constraints'] = {'date': column.format}
        try:
            part['populates'] = get_column_populates(column, schema_name)
        except AttributeError:
            pass
        parts.append(part)

    return {"dataSetSLIManifest": {"parts": parts,
                                   "file": CSV_DATA_FILENAME,
                                   "dataSet": 'dataset.%s' % to_identifier(schema_name),
                                   "csvParams": {"quoteChar": '"',
                                                 "escapeChar": '"',
                                                 "separatorChar": ",",
                                                 "endOfLine": "\n"
                                                 }}}
