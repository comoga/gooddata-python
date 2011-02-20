import unittest

from gooddataclient import maql

from tests import logger


class TestMaql(unittest.TestCase):

    def test_date(self):
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE"', maql.get_date())
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "test", TITLE "Test");\n\n',
                          maql.get_date('Test'))


if __name__ == '__main__':
    unittest.main()
