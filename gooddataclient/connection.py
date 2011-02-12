import urllib2
import cookielib
import simplejson as json
import logging

from gooddataclient.project import Project
from gooddataclient.exceptions import AuthenticationError, ProjectNotFoundError

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

    DEFAULT_HOST = 'https://secure.gooddata.com'
    WEBDAV_HOST = 'https://secure-di.gooddata.com'

    LOGIN_URI = '/gdc/account/login'
    TOKEN_URI = '/gdc/account/token'
    MD_URI = '/gdc/md/'

    JSON_HEADERS = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'gooddata-python/0.1'
    }

    def __init__(self, username, password, debug=0):
        webdav_handler = self.get_webdav_handler(username, password)
        self.setup_urllib2([webdav_handler], debug)
        self.login(username, password)

    def get_webdav_handler(self, username, password):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self.WEBDAV_HOST, username, password)
        return urllib2.HTTPBasicAuthHandler(passman)

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


    def get_metadata(self):
        return self.request(self.MD_URI)

    def request(self, uri, data=None, host=DEFAULT_HOST, headers=JSON_HEADERS, method=None):
        logger.debug(uri)
        #data to json
        if data and isinstance(data, dict):
            data = json.dumps(data)
        #uri with host
        full_uri = ''.join((host, uri))
        #updating headers
        headers['Content-Length'] = str(len(data)) if data else 0
        request = RequestWithMethod(method, full_uri, data, headers)
        try:
            response = urllib2.urlopen(request)
        except urllib2.URLError, err:
            #webdav "errors"
            if err.code in (201, 204, 207):
                return True
            else:
                logger.error(err)
                raise
        if response.info().gettype() == 'application/json':
            return json.loads(response.read())
        return response


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

