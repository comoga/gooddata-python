import os
import time
import urllib2
import logging

from gooddataclient.exceptions import ProjectNotOpenedError, UploadFailed
from gooddataclient import text, maql

logger = logging.getLogger("gooddataclient")

class Project(object):

    PROJECTS_URI = '/gdc/projects'
    DATASETS_URI = '/gdc/md/%s/data/sets'
    MAQL_EXEC_URI = '/gdc/md/%s/ldm/manage'
    PULL_URI = '/gdc/md/%s/etl/pull'

    def __init__(self, connection, id):
        self.connection = connection
        self.id = id

    def delete(self):
        """Delete a GoodData project"""
        try:
            uri = '/'.join((self.PROJECTS_URI, self.id))
            self.connection.request(uri, method='DELETE')
        except (TypeError, urllib2.URLError):
            raise ProjectNotOpenedError()

    def execute_maql(self, maql):
        data = {'manage': {'maql': maql}}
        try:
            response = self.connection.request(self.MAQL_EXEC_URI % self.id, data)
        except urllib2.URLError:
            return False
        if len(response['uris']) > 0:
            return True
        return False

    def integrate_uploaded_data(self, dir_name, wait_for_finish=True):
        response = self.connection.request(self.PULL_URI % self.id,
                                           {'pullIntegration': dir_name})
        task_uri = response['pullTask']['uri']
        # checkLoadingStatus in AbstractConnector.java
        if wait_for_finish:
            while True:
                status = self.connection.request(task_uri)['taskStatus']
                logger.debug(status)
                if status == 'OK':
                    break
                if status in ('ERROR', 'WARNING'):
                    raise UploadFailed(status)
                time.sleep(0.5)

    def create_date_dimension(self, name=None, include_time=False):
        # TODO: check if not already created, if yes, do nothing
        date_maql = maql.get_date(name, include_time)
        self.execute_maql(date_maql)
        if not include_time:
            return
        data = open(os.path.join(os.path.dirname(__file__), 'resources',
                                  'connector', 'data.csv')).read()
        sli_manifest = open(os.path.join(os.path.dirname(__file__), 'resources',
                                         'connector', 'upload_info.json')).read()
        sli_manifest = sli_manifest.replace('%id%', text.to_identifier(name)).replace('%name%', name)
        dir_name = self.connection.webdav.upload(data, sli_manifest)
        self.integrate_uploaded_data(dir_name, wait_for_finish=True)
        self.connection.webdav.delete(dir_name)

