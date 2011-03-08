import unittest

from gooddataclient.project import Project
from gooddataclient.manifest import get_sli_manifest

from tests import logger, examples


class TestManifest(unittest.TestCase):

    def test_sli_manifest(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(Project(None))
            sli_manifest = get_sli_manifest(dataset.column_list,
                                            dataset.schema_name)
            self.assertEqual(example.sli_manifest, sli_manifest)


if __name__ == '__main__':
    unittest.main()
