import unittest

from gooddataclient.connection import Connection
from gooddataclient.exceptions import AuthenticationError, ProjectNotOpenedError,\
    ProjectNotFoundError

from tests.credentials import username, password
from tests.test_project import TEST_PROJECT_NAME
from tests import logger



class TestConnection(unittest.TestCase):

    def test_login(self):
        if username == '' or password == '':
            raise AttributeError, 'Please provide your username and password to your GoodData account (into creadentials.py)'
        self.assertRaises(AuthenticationError, Connection, '', '')
        connection = Connection(username, password)
        self.assertEquals('', connection.request('/gdc/account/token').read())
        self.assert_(connection.webdav.request('/uploads'))

    def test_create_and_delete_project(self):
        self.connection = Connection(username, password, debug=0)
        #drop all the test projects:
        self.connection.delete_projects_by_name(TEST_PROJECT_NAME)
        project = self.connection.create_project(TEST_PROJECT_NAME)
        self.assert_(project is not None)
        self.assert_(project.id is not None)
        project.delete()
        self.assertRaises(ProjectNotOpenedError, project.delete)
        self.assertRaises(ProjectNotFoundError, self.connection.get_project, 
                          name=TEST_PROJECT_NAME)
    

if __name__ == '__main__':
    unittest.main()
