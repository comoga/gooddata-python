import os
import unittest
from zipfile import ZipFile

from gooddataclient.archiver import create_archive, write_tmp_csv_file, \
    csv_to_list

from tests import logger, examples


class TestArchiver(unittest.TestCase):

    def test_csv(self):
        for example in examples.examples:
            csv_filename = write_tmp_csv_file(example.data_list, example.sli_manifest)
            f = open(csv_filename, 'r')
            content = f.read()
            f.close()
            os.remove(csv_filename)
            self.assertEqual(csv_to_list(example.data_csv), csv_to_list(content))


    def test_archive(self):
        for example in examples.examples:
            filename = create_archive(example.data_csv, example.sli_manifest)
            zip_file = ZipFile(filename, "r")
            self.assertEquals(None, zip_file.testzip())
            self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
            zip_file.close()
            os.remove(filename)

    def test_csv_to_list(self):
        for example in examples.examples:
            data_list = csv_to_list(example.data_csv)
            self.assertEqual(example.data_list, data_list)


if __name__ == '__main__':
    unittest.main()
