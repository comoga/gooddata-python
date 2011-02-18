import os
import unittest
from zipfile import ZipFile
from xml.dom.minidom import parseString

from gooddataclient.archiver import create_archive, write_tmp_csv_file, \
    get_xml_schema, get_sli_manifest

from tests.examples import department 
from tests import logger


class TestArchiver(unittest.TestCase):

    def test_csv(self):
        csv_filename = write_tmp_csv_file(department.data_list, department.sli_manifest)
        f = open(csv_filename, 'r')
        content = f.read()
        f.close()
        os.remove(csv_filename)
        self.assertEqual(department.data_csv, content)


    def test_archive(self):
        filename = create_archive(department.data_csv, department.sli_manifest)
        zip_file = ZipFile(filename, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(filename)

    def test_schema(self):
        schema = parseString(department.schema_xml.replace(' ', '').replace('\n', ''))
        gen_schema = parseString(get_xml_schema(department.column_list, 'Department'))
        self.assertEqual(len(schema.childNodes), len(gen_schema.childNodes))
        self.assertEqual(len(schema.childNodes[0].childNodes), len(gen_schema.childNodes[0].childNodes), 
                         '%s != %s' % (', '.join(n.nodeName for n in schema.childNodes[0].childNodes),
                                       ', '.join(n.nodeName for n in gen_schema.childNodes[0].childNodes)))
        self.assertEqual(len(schema.childNodes[0].childNodes[1].childNodes), len(gen_schema.childNodes[0].childNodes[1].childNodes))

    def test_sli_manifest(self):
        sli_manifest = get_sli_manifest(department.column_list,
                                        'department', 'dataset.department')
        self.assertEqual(department.sli_manifest, sli_manifest)


if __name__ == '__main__':
    unittest.main()
