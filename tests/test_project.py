import unittest
from zipfile import ZipFile
import os

from gooddataclient.connection import Connection
from gooddataclient.archiver import write_tmp_file
from gooddataclient.exceptions import DataSetNotFoundError

from tests.credentials import password, username
from tests import examples_data
from tests import logger


TEST_PROJECT_NAME = 'gdc_unittest'

class TestProject(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password, debug=0)
        #drop all the test projects:
        self.connection.delete_projects_by_name(TEST_PROJECT_NAME)
        self.project = self.connection.create_project(TEST_PROJECT_NAME)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            self.connection.request('/uploads/%s/' % dir_name,
                                    host=Connection.WEBDAV_HOST,
                                    method='DELETE')

    def tearDown(self):
        self.project.delete()

    def test_create_structure(self):
        self.assertFalse(self.project.execute_maql('CREATE DATASET {dat'))
        self.assertRaises(DataSetNotFoundError, self.project.get_dataset,
                          name='Department')
        self.assert_(self.project.execute_maql(examples_data.maql))
        self.assert_(self.project.get_dataset(name='Department'))

    def test_transfer_data(self):
        self.project.execute_maql(examples_data.maql)
        dir_name = self.project.transfer(examples_data.data,
                                         examples_data.sli_manifest)
        self.assert_(len(dir_name) > 0)
        self.assert_(self.connection.request('/uploads/%s' % dir_name,
                                             host=Connection.WEBDAV_HOST))
        uploaded_file = self.connection.request('/uploads/%s/upload.zip' % dir_name,
                                                host=Connection.WEBDAV_HOST)
        tmp_file = write_tmp_file(uploaded_file.read())
        zip_file = ZipFile(tmp_file, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(tmp_file)
        dataset = self.project.get_dataset(name='Department')
        self.assert_(dataset['dataUploads'])
        self.assertEquals('OK', dataset['lastUpload']['dataUploadShort']['status'])
        self.connection.request('/uploads/%s/' % dir_name, host=Connection.WEBDAV_HOST,
                                method='DELETE')




if __name__ == '__main__':
    unittest.main()
