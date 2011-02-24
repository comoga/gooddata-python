import unittest
from xml.dom.minidom import parseString

from gooddataclient.schema import get_xml_schema

from tests import logger, examples


class TestSchema(unittest.TestCase):

    def test_xml_schema(self):
        for example in examples.examples:
            schema = parseString(example.schema_xml.replace(' ', '').replace('\n', ''))
            gen_schema = parseString(get_xml_schema(example.ExampleDataset.column_list,
                                                    example.ExampleDataset.schema_name))
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


if __name__ == '__main__':
    unittest.main()
