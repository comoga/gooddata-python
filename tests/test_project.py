import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.dataset import DateDimension
from gooddataclient.exceptions import DataSetNotFoundError,\
    ProjectNotOpenedError, ProjectNotFoundError, MaqlExecutionFailed

from tests.credentials import password, username
from tests import logger, examples


TEST_PROJECT_NAME = 'gdc_unittest'

class TestProject(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password, debug=0)
        #drop all the test projects:
        delete_projects_by_name(self.connection, TEST_PROJECT_NAME)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            self.connection.delete_webdav_dir(dir_name)

    def test_create_and_delete_project(self):
        project = Project(self.connection).create(TEST_PROJECT_NAME)
        self.assert_(project is not None)
        self.assert_(project.id is not None)
        project.delete()
        self.assertRaises(ProjectNotOpenedError, project.delete)
        self.assertRaises(ProjectNotFoundError, project.load, 
                          name=TEST_PROJECT_NAME)

    def test_create_structure(self):
        project = Project(self.connection).create(TEST_PROJECT_NAME)
        self.assertRaises(MaqlExecutionFailed, project.execute_maql, 'CREATE DATASET {dat')
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(project)
            self.assertRaises(DataSetNotFoundError, dataset.get_metadata,
                              name=dataset.schema_name)
            dataset.create()
            self.assert_(dataset.get_metadata(name=dataset.schema_name))
        project.delete()

if __name__ == '__main__':
    unittest.main()
