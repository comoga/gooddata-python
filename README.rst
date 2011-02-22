GoodData client library written in python.

Requires
========
* simplejson <http://pypi.python.org/pypi/simplejson/>

Usage
=====
Basic scenario::

	connection = Connection(username, password)
	project = connection.get_project('project_name')
	project.upload_dataset(maql, data, sli_manifest)

Executing the Forex example::

	from tests.examples import forex as example
	connection = Connection(username, password)
	project = connection.get_project('forex')
	project.create_date_dimension(name='Forex', include_time=True)
	sli_manifest = get_sli_manifest(example.column_list, example.schema_name,
                                    example.dataset_id)
	project.upload_dataset(example.maql, example.data, sli_manifest)

Working with the project::

	connection = Connection(username, password)
	project = connection.get_project('project_name')
	project = connection.create_project('project_name')
	project.delete()
	connection.delete_projects_by_name('project_name')

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
