GoodData client library written in python.

Requires
========
* simplejson <http://pypi.python.org/pypi/simplejson/>

Usage
=====
Basic scenario::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project
	from something import SomeDataset

	connection = Connection(username, password)
	project = Project(connection).load(name='project_name')
	dataset = SomeDataset(project)
	dataset.upload()

Executing the Forex example::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project
	from tests.examples import forex

	connection = Connection(username, password)
	project = Project(connection).load(name='forex')
	dataset = forex.Forex(project)
	dataset.upload()

Setting up the Dataset for Forex example::

	from gooddataclient.dataset import Dataset

	class Forex(Dataset):
	
	    id = ConnectionPoint(title='Id', dataType='IDENTITY')
	    time = Date(title='TIME', datetime=True, folder='Forex', schemaReference='Forex', format='dd-MM-yyyy HH:mm:ss')
	    volume = Fact(title='VOLUME', dataType='DECIMAL(8,4)', folder='Forex')
	    open = Fact(title='OPEN', dataType='DECIMAL(8,4)', folder='Forex')
	    close = Fact(title='CLOSE', dataType='DECIMAL(8,4)', folder='Forex')
	    min = Fact(title='MIN', dataType='DECIMAL(8,4)', folder='Forex')
	    max = Fact(title='MAX', dataType='DECIMAL(8,4)', folder='Forex')
	
	    class Meta:
	        column_order = ('id', 'time', 'volume', 'open', 'close', 'min', 'max')
	
	    @property
	    def schema_name(self):
	        return 'forex'
	
	    def data(self):
	        return [{'min': '1.0019', 'max': '1.0026', 'volume': '140', 'time_dt': '40485', 'time': '04-11-2010 00:48:01', 'time_tm': '2881', 'close': '1.0022', 'tm_time_id': '2881', 'open': '1.0023', 'id': 'a4aea808c4d9fc2a11771e7087177546'},
	                {'min': '1.0017', 'max': '1.0024', 'volume': '182', 'time_dt': '40485', 'time': '04-11-2010 00:49:01', 'time_tm': '2941', 'close': '1.0022', 'tm_time_id': '2941', 'open': '1.0024', 'id': 'f610d2a7e98bf4a2d1d40f3ba391effb'},
	                {'min': '1.0018', 'max': '1.0025', 'volume': '198', 'time_dt': '40485', 'time': '04-11-2010 00:50:01', 'time_tm': '3001', 'close': '1.0023', 'tm_time_id': '3001', 'open': '1.0022', 'id': 'a0c81959893ee94b19b8183a638e0ce6'}
	                ]

This shows how you can make a Dataset object. It defines the dataset in the 
similar way like XML schema in Java client but it's written like Django model. 
column_order is necessary to execute columns in the proper order, because you 
can't get python attributes in any particular sorted order (is't internally 
saved like a dictionary). Name of the columns is the name of the attribute. 
Schema name (the dataset title) is derived from the name of the class, but that can be 
changed if needed (shown above). The data() method is used to retrieve data 
dynamically - created for further use by Django values() method on model managers.  

Working with the project::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project, delete_projects_by_name

	connection = Connection(username, password)
	project = Project(connection)
	project = project.load(name='project_name')
	project = project.create('project_name')
	project.delete()
	delete_projects_by_name(connection, 'project_name')

Features
========
* Logging in to the GoodData REST API and WebDav 
* Project creation, opening and deletion
* Execution of MAQL
* Uploading CSV data in a zip archive with a json manifest file into a WebDav
* Creating JSON manifest
* Creating MAQL for DATE dimension
* Creating the TimeDimension (MAQL and data)
* Support for generating MAQL create for Datasets

Tests
=====
Remember to put your username and password to the ``credentials.py`` file. 
The tests are running directly against a live GoodData API.

To-do
=====
* Creating Metrics, Reports and Dashboards if possible 
* Executing all examples from GoodData-CL
* Use original data sources (CSV files) in examples
* Packaging
