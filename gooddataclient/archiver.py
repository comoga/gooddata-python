import os
import csv
import simplejson as json
from tempfile import mkstemp
from zipfile import ZipFile
import datetime
import hashlib
from xml.dom.minidom import parseString

from gooddataclient.text import to_identifier

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

def write_tmp_csv_file(csv_data, sli_manifest):
    '''Write a CSV temporary file with values in csv_data - list of dicts.
    
    @param csv_data: list of dicts
    @param sli_manifest: json sli_manifest
    '''
    fieldnames = [part['columnName'] for part in sli_manifest['dataSetSLIManifest']['parts']]
    fp, filename = mkstemp()
    file = open(filename, 'w+b')
    writer = csv.DictWriter(file, fieldnames=fieldnames,
                            delimiter=sli_manifest['dataSetSLIManifest']['csvParams']['separatorChar'],
                            quotechar=sli_manifest['dataSetSLIManifest']['csvParams']['quoteChar'],
                            quoting=csv.QUOTE_ALL)
    headers = dict((n, n) for n in fieldnames)
    writer.writerow(headers)
    for line in csv_data:
        for key in fieldnames:
            #some incredible magic with additional date field
            if not key in line and key.endswith('_dt'):
                h = hashlib.md5()
                #h.update(line[key[:-3]])
                h.update('123456')
                line[key] = h.hexdigest()[:6]
            #formatting the date properly
            if isinstance(line[key], datetime.datetime):
                line[key] = line[key].strftime("%Y-%m-%d")
            #make 0/1 from bool
            if isinstance(line[key], bool):
                line[key] = int(line[key])
        writer.writerow(line)
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
    if isinstance(data, list):
        data_path = write_tmp_csv_file(data, sli_manifest)
    else:
        data_path = write_tmp_file(data)
    sli_manifest_path = write_tmp_file(json.dumps(sli_manifest))
    filename = write_tmp_zipfile((
                   (data_path, CSV_DATA_FILENAME),
                   (sli_manifest_path, DLI_MANIFEST_FILENAME),
                    ))
    os.remove(data_path)
    os.remove(sli_manifest_path)
    return filename

def csv_to_list(data_csv):
    '''Create list of dicts from CSV string.
    
    @param data_csv: CSV in a string
    '''
    reader = csv.reader(data_csv.strip().split('\n'))
    header = reader.next()
    data_list = []
    for line in reader:
        l = {}
        for i, value in enumerate(header):
            l[value] = line[i]
        data_list.append(l)
    return data_list

def get_xml_schema(column_list, schema_name):
    '''Create XML schema from list of columns in dicts. It's used to create
    MAQL through the Java Client.
    
    The column_list looks like this: 
    [{'name': 'id', 'title': 'Id', 'ldmType': 'ATTRIBUTE', 'folder': 'X'},
     {'name': 'price', 'title': 'Price', 'ldmType': 'FACT', 'dataType': 'DECIMAL', 'folder': 'X'},
    ...
    ]
    
    @param column_list: List of columns
    @param schema_name: name of the schema
    '''
    dom = parseString('<schema><name>%s</name><columns></columns></schema>' % schema_name)
    for column in column_list:
        xmlcol = dom.createElement('column')
        for key, val in column.iteritems():
            k = dom.createElement(key)
            v = dom.createTextNode(val)
            k.appendChild(v)
            xmlcol.appendChild(k)
        dom.childNodes[0].childNodes[1].appendChild(xmlcol)
    return dom.toxml()


