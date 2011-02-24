import os
import urllib2
import cookielib
import simplejson as json
import logging

from gooddataclient.exceptions import AuthenticationError
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
        self.webdav = Webdav(username, password)
        self.setup_urllib2(debug)
        self.login(username, password)

    def setup_urllib2(self, debug):
        handlers = [urllib2.HTTPCookieProcessor(cookielib.CookieJar()),
                    urllib2.HTTPHandler(debuglevel=debug),
                    urllib2.HTTPSHandler(debuglevel=debug),
                    ]
        handlers.append(self.webdav.get_handler())
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


class Webdav(Connection):

    HOST = 'https://secure-di.gooddata.com'
    UPLOADS_URI = '/uploads/%s/'

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_handler(self):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self.HOST, self.username, self.password)
        return urllib2.HTTPBasicAuthHandler(passman)

    def request(self, *args, **kwargs):
        try:
            return super(Webdav, self).request(*args, **kwargs)
        except urllib2.URLError, err:
            if hasattr(err, 'code') and err.code in (201, 204, 207):
                return
            raise

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

    def delete(self, dir_name):
        self.request(self.UPLOADS_URI % dir_name, method='DELETE')

