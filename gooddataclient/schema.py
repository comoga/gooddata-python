from xml.dom.minidom import parseString

def get_xml_schema(column_list, schema_name):
    '''Create XML schema from list of columns in dicts. It's used to create
    MAQL through the Java Client.
    
    The column_list looks like this: 
    [{'name': 'id', 'title': 'Id', 'ldmType': 'ATTRIBUTE', 'folder': 'X'},
     {'name': 'price', 'title': 'Price', 'ldmType': 'FACT', 'dataType': 'DECIMAL', 'folder': 'X'},
    ...
    ]
    
    @param column_list: List of columns
    @param schema_name: name of the schema
    '''
    dom = parseString('<schema><name>%s</name><columns></columns></schema>' % schema_name)
    for column in column_list:
        xmlcol = dom.createElement('column')
        for key, val in column.iteritems():
            k = dom.createElement(key)
            v = dom.createTextNode(val)
            k.appendChild(v)
            xmlcol.appendChild(k)
        dom.childNodes[0].childNodes[1].appendChild(xmlcol)
    return dom.toxml()