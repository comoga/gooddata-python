GoodData client library written in python.

Requires
========
* simplejson <http://pypi.python.org/pypi/simplejson/>

Usage
=====
Basic scenario implemented now::

	connection = Connection(username, password)
	project = connection.get_project('project_name')
	project.execute_maql(maql)
	project.transfer(data, sli_manifest)

Working with the project::

	connection = Connection(username, password)
	project = connection.get_project('project_name')
	project = connection.create_project('project_name')
	project.delete()
	connection.delete_projects_by_name('project_name')

Project operations are handled directly by connection, only the delete of the current project is handled by a project itself.

Features
========
* Logging in to the GoodData REST API and WebDav 
* Project creation, opening and deletion
* Execution of MAQL
* Uploading CSV data in a zip archive with a json manifest file into a WebDav 

Tests
=====
Remember to put your username and password to the ``credentials.py`` file. 
The tests are running against a live GoodData API and not a mock one.

To-do
=====
* Support for generating MAQL, JSON manifest and other features available in GoodData-CL 
* Executing examples from GoodData-CL
* Packaging
