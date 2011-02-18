import os
import unittest
from zipfile import ZipFile
from xml.dom.minidom import parseString

from gooddataclient.archiver import create_archive, write_tmp_csv_file, \
    get_xml_schema

from tests import examples_data
from tests import logger


class TestArchiver(unittest.TestCase):

    def test_csv(self):
        csv_filename = write_tmp_csv_file(examples_data.data_list, examples_data.sli_manifest)
        f = open(csv_filename, 'r')
        content = f.read()
        f.close()
        os.remove(csv_filename)
        self.assertEqual(examples_data.data, content)


    def test_archive(self):
        filename = create_archive(examples_data.data, examples_data.sli_manifest)
        zip_file = ZipFile(filename, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(filename)

    def test_schema(self):
        schema = parseString(examples_data.schema)
        gen_schema = parseString(get_xml_schema(examples_data.columns, 'DealOrder'))
        self.assertEqual(len(schema.childNodes), len(gen_schema.childNodes))
        self.assertEqual(len(schema.childNodes[0].childNodes), len(gen_schema.childNodes[0].childNodes))
        self.assertEqual(len(schema.childNodes[0].childNodes[1].childNodes), len(gen_schema.childNodes[0].childNodes[1].childNodes))



if __name__ == '__main__':
    unittest.main()
