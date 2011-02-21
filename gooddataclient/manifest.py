from gooddataclient import text
from gooddataclient.archiver import CSV_DATA_FILENAME

def get_column_populates(column, schema_name):
    schema_name_id = text.to_identifier(schema_name)
    column_name_id = text.to_identifier(column['name'])
    if column['ldmType'] in ('ATTRIBUTE', 'CONNECTION_POINT'):
        return ["label.%s.%s" % (schema_name_id, column_name_id)]
    if column['ldmType'] in ('LABEL'):
        return ["label.%s.%s.%s" % (schema_name_id,
                                    text.to_identifier(column['reference']),
                                    column_name_id)]
    if column['ldmType'] in ('REFERENCE'):
        return ["label.%s.%s" % (text.to_identifier(column['schemaReference']),
                                 text.to_identifier(column['reference']))]
    if column['ldmType'] in ('FACT'):
        return ["fact.%s.%s" % (schema_name_id, column_name_id)]
    if column['ldmType'] in ('DATE'):
        return ["%s.date.mdyy" % text.to_identifier(column['schemaReference'])]
    raise AttributeError, 'Nothing to populate'

def get_date_dt_column(column, schema_name):
    name = '%s_dt' % column['name']
    populates = 'dt.%s.%s' % (text.to_identifier(schema_name), text.to_identifier(column['name']))
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_time_tm_column(column, schema_name):
    name = '%s_tm' % column['name']
    populates = 'tm.dt.%s.%s' % (text.to_identifier(schema_name), text.to_identifier(column['name']))
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL'}

def get_tm_time_id_column(column, schema_name):
    name = 'tm_%s_id' % text.to_identifier(column['name'])
    populates = 'label.time.second.of.day.%s' % text.to_identifier(column['schemaReference'])
    return {'populates': [populates], 'columnName': name, 'mode': 'FULL', 'referenceKey': 1}

def get_sli_manifest(column_list, schema_name, dataset_id):
    '''Create JSON manifest from columns in schema.
    
    @param column_list: list of dicts (created from XML schema)
    @param schema_name: string
    @param dataset_id: string
    
    See populateColumnsFromSchema in AbstractConnector.java
    '''
    parts = []
    for column in column_list:
        # special additional column for date
        if column['ldmType'] == 'DATE':
            parts.append(get_date_dt_column(column, schema_name))
            if 'datetime' in column:
                parts.append(get_time_tm_column(column, schema_name))
                parts.append(get_tm_time_id_column(column, schema_name))


        part = {"columnName": column['name'],
                "mode": "FULL",
                }
        if column['ldmType'] in ('ATTRIBUTE', 'CONNECTION_POINT', 'REFERENCE',
                                 'DATE'):
            part["referenceKey"] = 1
        if 'format' in column:
            part['constraints'] = {'date': column['format']}
        try:
            part['populates'] = get_column_populates(column, schema_name)
        except AttributeError:
            pass
        parts.append(part)

    return {"dataSetSLIManifest": {"parts": parts,
                                   "file": CSV_DATA_FILENAME,
                                   "dataSet": dataset_id,
                                   "csvParams": {"quoteChar": '"',
                                                 "escapeChar": '"',
                                                 "separatorChar": ",",
                                                 "endOfLine": "\n"
                                                 }}}
