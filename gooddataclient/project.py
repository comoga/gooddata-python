import os
import time
import urllib2
import logging

from gooddataclient.exceptions import ProjectNotOpenedError, UploadFailed,\
    ProjectNotFoundError, MaqlExecutionFailed

logger = logging.getLogger("gooddataclient")

def delete_projects_by_name(connection, name):
    """Delete all GoodData projects by that name"""
    logger.debug('Dropping project by name %s' % name)
    try:
        while True:
            Project(connection).load(name=name).delete()
    except ProjectNotFoundError:
        pass



class Project(object):

    PROJECTS_URI = '/gdc/projects'
    DATASETS_URI = '/gdc/md/%s/data/sets'
    MAQL_EXEC_URI = '/gdc/md/%s/ldm/manage'
    PULL_URI = '/gdc/md/%s/etl/pull'

    def __init__(self, connection):
        self.connection = connection

    def load(self, id=None, name=None):
        self.id = id or self.get_id_by_name(name)
        return self

    def get_id_by_name(self, name):
        """Retrieve the project identifier"""
        data = self.connection.get_metadata()
        for link in data['about']['links']:
            if link['title'] == name:
                logger.debug('Retrieved Project identifier for %s: %s' % (name, link['identifier']))
                return link['identifier']
        raise ProjectNotFoundError('Failed to retrieve Project identifier for %s' % (name))

    def create(self, name, desc=None, template_uri=None):
        """Create a new GoodData project"""
        request_data = {'project': {'meta': {'title': name,
                                             'summary': desc,
                                             },
                                    'content': {'guidedNavigation': '1',
                                                },
                                    }}
        if template_uri:
            request_data['project']['meta']['projectTemplate'] = template_uri

        response = self.connection.request(Project.PROJECTS_URI, request_data)
        id = response['uri'].split('/')[-1]
        logger.debug("Created project name=%s with id=%s" % (name, id))
        return self.load(id=id)

    def delete(self):
        """Delete a GoodData project"""
        try:
            uri = '/'.join((self.PROJECTS_URI, self.id))
            self.connection.request(uri, method='DELETE')
        except (TypeError, urllib2.URLError):
            raise ProjectNotOpenedError()

    def execute_maql(self, maql):
        if not maql:
            raise AttributeError('MAQL missing, nothing to execute')
        data = {'manage': {'maql': maql}}
        try:
            response = self.connection.request(self.MAQL_EXEC_URI % self.id, data)
            if len(response['uris']) == 0:
                raise MaqlExecutionFailed
        except urllib2.URLError, msg:
            logger.debug(msg)
            raise MaqlExecutionFailed

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


