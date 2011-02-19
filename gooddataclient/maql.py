from gooddataclient.text import to_identifier


def get_date(name=None):
    '''Get MAQL for date dimension.
    
    See generateMaqlCreate in DateDimensionConnect.java
    '''
    if not name:
        return 'INCLUDE TEMPLATE "URN:GOODDATA:DATE"'

    maql = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");\n\n' % \
        (to_identifier(name), name)

    return maql
