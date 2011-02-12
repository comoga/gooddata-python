import os
import time
import urllib2
import logging

from gooddataclient.archiver import create_archive, DEFAULT_ARCHIVE_NAME
from gooddataclient.exceptions import ProjectNotOpenedError,\
    DataSetNotFoundError, UploadFailed

logger = logging.getLogger("gooddataclient")

class Project(object):

    PROJECTS_URI = '/gdc/projects'
    DATASETS_URI = '/gdc/md/%s/data/sets'
    MAQL_EXEC_URI = '/gdc/md/%s/ldm/manage'
    WEBDAV_URI = '/uploads/'
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

    def get_datasets(self):
        return self.connection.request(self.DATASETS_URI % self.id)
    
    def get_dataset(self, name):
        response = self.get_datasets()
        for dataset in response['dataSetsInfo']['sets']:
            if dataset['meta']['title'] == name:
                return dataset
        raise DataSetNotFoundError('DataSet %s not found' % name)
    
    def delete_dataset(self, name):
        dataset = self.get_dataset(name)
        return self.connection.request(dataset['meta']['uri'], method='DELETE')


    def transfer(self, data, sli_manifest, wait_for_finish=True):
        '''Create zip file with data in csv format and manifest file, then create
        directory in webdav and upload the zip file there. After it's uploaded,
        send a command to GoodData to begin the integration.
        
        @param data: csv data to upload
        @param sli_manifest: dictionary with the columns definitions
        @param wait_for_finish: check periodically for the integration result
        
        return the name of the temporary file, hence the name of the directory
        created in webdav uploads folder
        '''
        filename = create_archive(data, sli_manifest)
        dir_name = os.path.basename(filename)
        self.connection.request(''.join((self.WEBDAV_URI, dir_name)), 
                                host=self.connection.WEBDAV_HOST, method='MKCOL')
        f = open(filename, 'rb')
        # can it be streamed?
        self.connection.request(''.join((self.WEBDAV_URI, dir_name, '/', DEFAULT_ARCHIVE_NAME)),
                                host=self.connection.WEBDAV_HOST, data=f.read(), 
                                headers={'Content-Type': 'application/zip'},
                                method='PUT')
        f.close()
        os.remove(filename)
        response = self.connection.request(self.PULL_URI % self.id, {'pullIntegration': dir_name})
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

        return dir_name

