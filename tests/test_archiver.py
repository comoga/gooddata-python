import os
import unittest
from zipfile import ZipFile

from gooddataclient.archiver import create_archive

from tests import examples_data
from tests import logger


class TestArchiver(unittest.TestCase):

    def test_archive(self):
        filename = create_archive(examples_data.data, examples_data.sli_manifest)
        zip_file = ZipFile(filename, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(filename)


if __name__ == '__main__':
    unittest.main()
