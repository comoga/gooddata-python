import os
import logging
import inspect

from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient import text
from gooddataclient.manifest import get_sli_manifest
from gooddataclient.maql import maql_dataset
from gooddataclient.columns import Column
from gooddataclient.text import to_identifier

logger = logging.getLogger("gooddataclient")

class Dataset(object):

    DATASETS_URI = '/gdc/md/%s/data/sets'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    @property
    def schema_name(self):
        return self.__class__.__name__

    class Meta:
        column_order = None


    def get_class_members(self):
        members = inspect.getmembers(self, lambda member: isinstance(member, Column))
        if not self.Meta.column_order:
            return members
        members_ordered = []
        for col_name in self.Meta.column_order:
            for name, column in members:
                if name == col_name:
                    members_ordered.append((name, column))
        return members_ordered

    def get_columns(self):
        columns = []
        for name, column in self.get_class_members():
            if not column.name:
                column.name = to_identifier(name)
            column.schema_name = to_identifier(self.schema_name)
            columns.append(column)
        return columns

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

    def get_date_dimension(self):
        for column in self.column_list:
            if column['ldmType'] == 'DATE':
                return column

    def create(self):
        date_dimension = self.get_date_dimension()
        if date_dimension:
            DateDimension(self.project).create(name=date_dimension['schemaReference'],
                                               include_time=('datetime' in date_dimension))
        self.project.execute_maql(self.get_maql())

    def upload(self):
        try:
            self.get_metadata(self.schema_name)
        except DataSetNotFoundError:
            self.create()
        sli_manifest = get_sli_manifest(self.column_list, self.schema_name)
        dir_name = self.connection.webdav.upload(self.data(), sli_manifest)
        self.project.integrate_uploaded_data(dir_name)
        self.connection.webdav.delete(dir_name)

    def get_maql(self):
        return maql_dataset(self.schema_name, self.column_list)


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

