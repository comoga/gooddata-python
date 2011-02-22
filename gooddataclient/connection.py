import os
import urllib2
import cookielib
import simplejson as json
import logging

from gooddataclient.project import Project
from gooddataclient.exceptions import AuthenticationError, ProjectNotFoundError
from gooddataclient.archiver import create_archive, DEFAULT_ARCHIVE_NAME

logger = logging.getLogger("gooddataclient")

class RequestWithMethod(urllib2.Request):

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        if self._method:
            return self._method
        elif self.has_data():
            return 'POST'
        else:
            return 'GET'



class Connection(object):

    HOST = 'https://secure.gooddata.com'

    LOGIN_URI = '/gdc/account/login'
    TOKEN_URI = '/gdc/account/token'
    MD_URI = '/gdc/md/'

    JSON_HEADERS = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'gooddata-python/0.1'
    }

    def __init__(self, username, password, debug=0):
        self.webdav = Webdav()
        self.setup_urllib2([self.webdav.get_handler(username, password)], debug)
        self.login(username, password)

    def setup_urllib2(self, additional_handlers=[], debug=0):
        handlers = [urllib2.HTTPCookieProcessor(cookielib.CookieJar()),
                    urllib2.HTTPHandler(debuglevel=debug),
                    urllib2.HTTPSHandler(debuglevel=debug),
                    ] + additional_handlers
        opener = urllib2.build_opener(*handlers)
        urllib2.install_opener(opener)

    def login(self, username, password):
        try:
            request_data = {'postUserLogin': {
                              'login': username,
                              'password': password,
                              'remember': 1,
                              }}
            response = self.request(self.LOGIN_URI, request_data)
            response['userLogin']['profile']
            self.request(self.TOKEN_URI)
        except urllib2.URLError:
            raise AuthenticationError('Please provide correct username and password.')
        except KeyError:
            raise AuthenticationError('No userLogin information in response to login.')

    def request(self, uri, data=None, headers=JSON_HEADERS, method=None):
        logger.debug(uri)
        #data to json
        if data and isinstance(data, dict):
            data = json.dumps(data)
        #uri with host
        full_uri = ''.join((self.HOST, uri))
        #updating headers
        headers['Content-Length'] = str(len(data)) if data else 0
        request = RequestWithMethod(method, full_uri, data, headers)
        try:
            response = urllib2.urlopen(request)
        except urllib2.URLError, err:
            logger.error(err)
            raise
        if response.info().gettype() == 'application/json':
            return json.loads(response.read())
        return response

    def get_metadata(self):
        return self.request(self.MD_URI)

    def get_project(self, id=None, name=None):
        id = id or self.get_project_id_by_name(name)
        return Project(self, id)

    def get_project_id_by_name(self, name):
        """Retrieve the project identifier"""
        data = self.get_metadata()
        for link in data['about']['links']:
            if link['title'] == name:
                logger.debug('Retrieved Project identifier for %s: %s' % (name, link['identifier']))
                return link['identifier']
        raise ProjectNotFoundError('Failed to retrieve Project identifier for %s' % (name))

    def create_project(self, name, desc=None, template_uri=None):
        """Create a new GoodData project"""
        request_data = {'project': {'meta': {'title': name,
                                             'summary': desc,
                                             },
                                    'content': {'guidedNavigation': '1',
                                                },
                                    }}
        if template_uri:
            request_data['project']['meta']['projectTemplate'] = template_uri

        response = self.request(Project.PROJECTS_URI, request_data)
        id = response['uri'].split('/')[-1]
        logger.debug("Created project name=%s with id=%s" % (name, id))
        return self.get_project(id=id)

    def delete_projects_by_name(self, name):
        """Delete all GoodData projects by that name"""
        logger.debug('Dropping project by name %s' % name)
        try:
            while True:
                project = self.get_project(name=name)
                project.delete()
        except ProjectNotFoundError:
            pass


class Webdav(Connection):

    HOST = 'https://secure-di.gooddata.com'
    UPLOADS_URI = '/uploads/%s/'

    def __init__(self):
        pass

    def get_handler(self, username, password):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self.HOST, username, password)
        return urllib2.HTTPBasicAuthHandler(passman)

    def request(self, *args, **kwargs):
        try:
            return super(Webdav, self).request(*args, **kwargs)
        except urllib2.URLError, err:
            if hasattr(err, 'code') and err.code in (201, 204, 207):
                return
            raise

    def delete(self, dir_name):
        self.request(self.UPLOADS_URI % dir_name, method='DELETE')

    def upload(self, data, sli_manifest):
        '''Create zip file with data in csv format and manifest file, then create
        directory in webdav and upload the zip file there. 
        
        @param data: csv data to upload
        @param sli_manifest: dictionary with the columns definitions
        @param wait_for_finish: check periodically for the integration result
        
        return the name of the temporary file, hence the name of the directory
        created in webdav uploads folder
        '''
        filename = create_archive(data, sli_manifest)
        dir_name = os.path.basename(filename)
        self.request(self.UPLOADS_URI % dir_name, method='MKCOL')
        f = open(filename, 'rb')
        # can it be streamed?
        self.request(''.join((self.UPLOADS_URI % dir_name, DEFAULT_ARCHIVE_NAME)),
                     data=f.read(), headers={'Content-Type': 'application/zip'},
                     method='PUT')
        f.close()
        os.remove(filename)
        return dir_name
