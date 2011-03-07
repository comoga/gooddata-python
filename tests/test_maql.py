import unittest
import difflib

from gooddataclient.project import Project
from gooddataclient.maql import maql_dataset

from tests import logger, examples

class TestMaql(unittest.TestCase):

    def test_dataset_maql(self):
        for example in examples.examples:
            dataset = example.ExampleDataset(Project(None))
            maql_generated = maql_dataset(dataset.schema_name, dataset.dataset_id, 
                                          dataset.column_list)
            diff = '\n'.join(difflib.unified_diff(maql_generated.splitlines(),
                                                  example.maql.splitlines(),
                                                  lineterm=''))
            self.assertEquals(example.maql, maql_generated, diff)

if __name__ == '__main__':
    unittest.main()
