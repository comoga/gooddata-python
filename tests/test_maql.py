import unittest

from gooddataclient import maql

from tests import logger
from tests.examples import forex


class TestMaql(unittest.TestCase):

    def test_date(self):
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE"', maql.get_date())
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "test", TITLE "Test");\n\n',
                          maql.get_date('Test'))
        self.assertEquals(forex.time_dimension, maql.get_date('Forex', include_time=True))
        self.assertEquals(forex.time_dimension.replace('forex', 'xerof').replace('Forex', 'Xerof'), 
                          maql.get_date('Xerof', include_time=True))
        

if __name__ == '__main__':
    unittest.main()
