import unittest
from zipfile import ZipFile
import os

from gooddataclient.connection import Connection
from gooddataclient.archiver import write_tmp_file
from gooddataclient.exceptions import DataSetNotFoundError
from gooddataclient import maql

from tests.credentials import password, username
from tests import logger, examples


TEST_PROJECT_NAME = 'gdc_unittest'

class TestProject(unittest.TestCase):

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

    def test_create_structure(self):
        self.assertFalse(self.project.execute_maql('CREATE DATASET {dat'))
        for example in examples.examples:
            self.assertRaises(DataSetNotFoundError, self.project.get_dataset,
                              name=example.schema_name)
            if hasattr(example, 'date_dimension'):
                self.assertFalse(self.project.execute_maql(example.maql), example.maql)
                self.project.create_date_dimension(name=example.date_dimension['name'],
                                                  include_time=('include_time' in example.date_dimension))
            self.assert_(self.project.execute_maql(example.maql), example.maql)
            self.assert_(self.project.get_dataset(name=example.schema_name))

    def test_create_date_dimension(self):
        for example in examples.examples:
            if not hasattr(example, 'date_dimension'):
                continue
            date_maql = maql.get_date(name=example.date_dimension['name'],
                                      include_time=('include_time' in example.date_dimension))
            self.assert_(self.project.execute_maql(date_maql), date_maql)
            if 'include_time' in example.date_dimension:
                self.project.create_date_dimension(example.date_dimension['name'])
                # TODO: verify the creation

    def test_upload_data(self):
        for example in examples.examples:
            if hasattr(example, 'date_dimension'):
                self.project.create_date_dimension(name=example.date_dimension['name'],
                                                  include_time=('include_time' in example.date_dimension))

            self.assert_(self.project.execute_maql(example.maql), example.maql)
            dir_name = self.project.upload_to_webdav(example.data_csv, example.sli_manifest)
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

            self.project.integrate_uploaded_data(dir_name)
            dataset = self.project.get_dataset(name=example.schema_name)
            self.assert_(dataset['dataUploads'])
            self.assertEquals('OK', dataset['lastUpload']['dataUploadShort']['status'])
            self.connection.delete_webdav_dir(dir_name)




if __name__ == '__main__':
    unittest.main()
