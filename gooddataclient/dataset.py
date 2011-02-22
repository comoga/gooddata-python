from gooddataclient.exceptions import DataSetNotFoundError


class Dataset(object):

    DATASETS_URI = '/gdc/md/%s/data/sets'

    def __init__(self, project):
        self.project = project
        self.connection = project.connection

    def get_datasets_metadata(self):
        return self.connection.request(self.DATASETS_URI % self.project.id)

    def get_metadata(self, name):
        response = self.get_datasets_metadata()
        for dataset in response['dataSetsInfo']['sets']:
            if dataset['meta']['title'] == name:
                return dataset
        raise DataSetNotFoundError('DataSet %s not found' % name)

    def delete(self, name):
        dataset = self.get_metadata(name)
        return self.connection.request(dataset['meta']['uri'], method='DELETE')

    def upload(self, maql, data, sli_manifest):
        # TODO: check if not already created, do not exec maql, but always upload
        self.project.execute_maql(maql)

        dir_name = self.connection.upload_to_webdav(data, sli_manifest)
        dir_name = self.project.integrate_uploaded_data(dir_name)
        self.connection.delete_webdav_dir(dir_name)
