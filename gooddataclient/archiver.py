import os
import simplejson as json
from tempfile import mkstemp
from zipfile import ZipFile


DLI_MANIFEST_FILENAME = 'upload_info.json'
CSV_DATA_FILENAME = 'data.csv'
DEFAULT_ARCHIVE_NAME = 'upload.zip'

def write_tmp_file(content):
    '''Write any data to a temporary file.
    Remember to os.remove(filename) after use.
    
    @param content: data to be written to a file
    
    return filename of the created temporary file
    '''
    fp, filename = mkstemp()
    file = open(filename, 'w+b')
    file.write(content)
    os.close(fp)
    return filename

def write_tmp_zipfile(files):
    '''Zip files into a single file.
    Remember to os.remove(filename) after use.
    
    @param files: list of tuples (path_to_the_file, name_of_the_file)
    
    return filename of the created temporary zip file
    '''
    fp, filename = mkstemp()
    zip_file = ZipFile(filename, "w")
    for path, name in files:
        zip_file.write(path, name)
    zip_file.close()
    os.close(fp)
    return filename


def create_archive(data, sli_manifest):
    '''Zip the data and sli_manifest files to an archive. 
    Remember to os.remove(filename) after use.
    
    @param data: csv data
    @param sli_manifest: json sli_manifest
    
    return the filename to the temporary zip file
    '''
    data_path = write_tmp_file(data)
    sli_manifest_path = write_tmp_file(json.dumps(sli_manifest))
    filename = write_tmp_zipfile((
                   (data_path, CSV_DATA_FILENAME),
                   (sli_manifest_path, DLI_MANIFEST_FILENAME),
                    ))
    os.remove(data_path)
    os.remove(sli_manifest_path)
    return filename
