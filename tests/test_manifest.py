import unittest

from gooddataclient.manifest import get_sli_manifest

from tests import logger, examples


class TestManifest(unittest.TestCase):

    def test_sli_manifest(self):
        for example in examples.examples:
            sli_manifest = get_sli_manifest(example.ExampleDataset.column_list,
                                            example.ExampleDataset.schema_name,
                                            example.ExampleDataset.dataset_id)
            self.assertEqual(example.sli_manifest, sli_manifest)


if __name__ == '__main__':
    unittest.main()
