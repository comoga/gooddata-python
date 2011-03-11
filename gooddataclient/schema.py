from xml.dom.minidom import parseString

def get_xml_schema(dataset):
    '''Create XML schema from list of columns in dicts. It's used to create
    MAQL through the Java Client.
    '''
    dom = parseString('<schema><name>%s</name><columns></columns></schema>' % dataset.schema_name)
    for column in dataset.get_columns():
        xmlcol = dom.createElement('column')
        for key, val in column.get_schema_values():
            k = dom.createElement(key)
            v = dom.createTextNode(val)
            k.appendChild(v)
            xmlcol.appendChild(k)
        dom.childNodes[0].childNodes[1].appendChild(xmlcol)
    return dom.toxml()