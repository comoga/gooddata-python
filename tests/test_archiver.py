import os
import unittest
from zipfile import ZipFile
from xml.dom.minidom import parseString

from gooddataclient.archiver import create_archive, write_tmp_csv_file, \
    get_xml_schema, csv_to_list

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

    def test_schema(self):
        for example in examples.examples:
            schema = parseString(example.schema_xml.replace(' ', '').replace('\n', ''))
            gen_schema = parseString(get_xml_schema(example.column_list, example.schema_name))
            # TODO: test for XML content (not so easily comparable)
            self.assertEqual(len(schema.childNodes), len(gen_schema.childNodes))
            self.assertEqual(len(schema.childNodes[0].childNodes), len(gen_schema.childNodes[0].childNodes), 
                             '%s != %s' % (', '.join(n.nodeName for n in schema.childNodes[0].childNodes),
                                           ', '.join(n.nodeName for n in gen_schema.childNodes[0].childNodes)))
            self.assertEqual(len(schema.childNodes[0].childNodes[1].childNodes),
                             len(gen_schema.childNodes[0].childNodes[1].childNodes), 
                             '%s != %s (%s)' % (', '.join(n.nodeName for n in schema.childNodes[0].childNodes[1].childNodes),
                                                ', '.join(n.nodeName for n in gen_schema.childNodes[0].childNodes[1].childNodes),
                                                example))

    def test_csv_to_list(self):
        for example in examples.examples:
            data_list = csv_to_list(example.data_csv)
            self.assertEqual(example.data_list, data_list)


if __name__ == '__main__':
    unittest.main()
