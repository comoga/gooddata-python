import os
import logging

from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient import text
from gooddataclient.manifest import get_sli_manifest

logger = logging.getLogger("gooddataclient")

class Dataset(object):

    DATASETS_URI = '/gdc/md/%s/data/sets'

    # Defined in child class
    date_dimension = None
    maql = None
    schema_name = None
    dataset_id = None
    column_list = None

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    def get_datasets_metadata(self):
        return self.connection.request(self.DATASETS_URI % self.project.id)

    def get_metadata(self, name):
        response = self.get_datasets_metadata()
        for dataset in response['dataSetsInfo']['sets']:
            if dataset['meta']['title'] == name:
                return dataset
        raise DataSetNotFoundError('DataSet %s not found' % name)

    def delete(self, name):
        dataset = self.get_metadata(name)
        return self.connection.request(dataset['meta']['uri'], method='DELETE')

    def data(self):
        raise NotImplementedError

    def upload(self):
        # TODO: check if not already created, do not exec maql, but always upload
        self.project.execute_maql(self.maql)

        sli_manifest = get_sli_manifest(self.column_list, self.schema_name, 
                                        self.dataset_id)
        dir_name = self.connection.webdav.upload(self.data(), sli_manifest)
        self.project.integrate_uploaded_data(dir_name)
        self.connection.webdav.delete(dir_name)


class DateDimension(object):

    DATE_MAQL = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE"'
    DATE_MAQL_ID = 'INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "%s", TITLE "%s");\n\n'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    def get_maql(self, name=None, include_time=False):
        '''Get MAQL for date dimension.
        
        See generateMaqlCreate in DateDimensionConnect.java
        '''
        if not name:
            return self.DATE_MAQL

        maql = self.DATE_MAQL_ID % (text.to_identifier(name), name)

        if include_time:
            file_path = os.path.join(os.path.dirname(__file__), 'resources',
                                     'connector', 'TimeDimension.maql')
            time_dimension = open(file_path).read()\
                                .replace('%id%', text.to_identifier(name))\
                                .replace('%name%', name)
            maql = ''.join((maql, time_dimension))

        return maql

    def create(self, name=None, include_time=False):
        # TODO: check if not already created, if yes, do nothing
        self.project.execute_maql(self.get_maql(name, include_time))
        if include_time:
            self.upload_time(name)
        return self

    def upload_time(self, name):
        data = open(os.path.join(os.path.dirname(__file__), 'resources',
                                  'connector', 'data.csv')).read()
        sli_manifest = open(os.path.join(os.path.dirname(__file__), 'resources',
                                         'connector', 'upload_info.json')).read()\
                         .replace('%id%', text.to_identifier(name))\
                         .replace('%name%', name)
        dir_name = self.connection.webdav.upload(data, sli_manifest)
        self.project.integrate_uploaded_data(dir_name, wait_for_finish=True)
        self.connection.webdav.delete(dir_name)

