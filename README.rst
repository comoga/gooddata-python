GoodData client library written in python.

Requires
========
* simplejson <http://pypi.python.org/pypi/simplejson/>

Usage
=====
Basic scenario::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project
	from gooddataclient.dataset import Dataset

	connection = Connection(username, password)
	project = Project(connection).load(name='project_name')
	SomeDataset(project).upload()

Executing the Forex example::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project
	from gooddataclient.dataset import Dataset, DateDimension
	from gooddataclient.manifest import get_sli_manifest
	from tests.examples import forex as example

	connection = Connection(username, password)
	project = Project(connection).load(name='forex')
	DateDimension(project).create(name='Forex', include_time=True)
	example.ExampleDataset(project).upload()

Working with the project::

	from gooddataclient.connection import Connection
	from gooddataclient.project import Project, delete_projects_by_name

	connection = Connection(username, password)
	project = Project(connection)
	project = project.load(name='project_name')
	project = project.create('project_name')
	project.delete()
	delete_projects_by_name(connection, 'project_name')

Project operations are handled directly by connection, only the deletion of the current project is handled by a project itself.

Features
========
* Logging in to the GoodData REST API and WebDav 
* Project creation, opening and deletion
* Execution of MAQL
* Uploading CSV data in a zip archive with a json manifest file into a WebDav
* Creating JSON manifest
* Creating MAQL for DATE dimension
* Creating the TimeDimension (MAQL and data)

Tests
=====
Remember to put your username and password to the ``credentials.py`` file. 
The tests are running against a live GoodData API and not a mock one.

To-do
=====
* Support for generating MAQL
* Executing all examples from GoodData-CL
* Packaging
