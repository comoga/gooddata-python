import unittest

from gooddataclient.connection import Connection
from gooddataclient.dataset import Dataset, DateDimension
from gooddataclient.exceptions import DataSetNotFoundError

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
        dataset = Dataset(self.project)
        for example in examples.examples:
            self.assertRaises(DataSetNotFoundError, dataset.get_metadata,
                              name=example.schema_name)
            if hasattr(example, 'date_dimension'):
                self.assertFalse(self.project.execute_maql(example.maql), example.maql)
                DateDimension(self.project).create(name=example.date_dimension['name'],
                                                   include_time=('include_time' in example.date_dimension))
            self.assert_(self.project.execute_maql(example.maql), example.maql)
            self.assert_(dataset.get_metadata(name=example.schema_name))

if __name__ == '__main__':
    unittest.main()
