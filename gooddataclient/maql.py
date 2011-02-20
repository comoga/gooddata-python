import os

from gooddataclient.text import to_identifier


def get_date(name=None, include_time=False):
    '''Get MAQL for date dimension.
    
    See generateMaqlCreate in DateDimensionConnect.java
    '''
    if not name:
        return 'INCLUDE TEMPLATE "URN:GOODDATA:DATE"'

    maql = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");\n\n' % \
        (to_identifier(name), name)

    if include_time:
        file_path = os.path.join(os.path.dirname(__file__), 'resources', 
                                 'connector', 'TimeDimension.maql')
        time_dimension = open(file_path).read()
        time_dimension = time_dimension.replace('%id%', to_identifier(name))
        time_dimension = time_dimension.replace('%name%', name)
        maql = ''.join((maql, time_dimension))

    return maql
