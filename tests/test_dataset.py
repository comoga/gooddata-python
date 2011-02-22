import unittest
from zipfile import ZipFile
import os

from gooddataclient.connection import Connection
from gooddataclient.archiver import write_tmp_file
from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient import maql

from tests.credentials import password, username
from tests import logger, examples
from tests.test_project import TEST_PROJECT_NAME

class TestDataset(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password, debug=0)
        #drop all the test projects:
        self.connection.delete_projects_by_name(TEST_PROJECT_NAME)
        self.project = self.connection.create_project(TEST_PROJECT_NAME)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            self.connection.delete_webdav_dir(dir_name)

    def tearDown(self):
        self.project.delete()


if __name__ == '__main__':
    unittest.main()
