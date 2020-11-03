import os, uuid
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeDirectoryClient, DataLakeFileClient

try:
    print("Start of the sample")
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    # Create the DataLakeServiceClient object which will be used for access to the data lake storage
    print("\nCreate data lake service...")
    service_client = DataLakeServiceClient.from_connection_string(conn_str=connect_str)
    
    # generate a random name for testing purpose
    fs_name = "testfs1"
    
    # create the filesystem
    filesystem_client = service_client.create_file_system(file_system=fs_name)
    print("Create a test filesystem named '{}'.".format(fs_name))
    
    # create a directory hierarchy
    dir_name = "testdir1/testdir2/testdir3/testdir4/testdir5"
    main_directory_client = filesystem_client.create_directory(dir_name)
    print("Creating a directory named '{}'.".format(dir_name))
    
    # locate 4'th directory for uploading
    directory_client = filesystem_client.get_directory_client("testdir1/testdir2/testdir3/testdir4")
    
    # prepare file for uploading
    curr_dirr = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(curr_dirr, 'data/IndianFoodDatasetCSV.csv')
    local_file = open(file_path,'rb')
    file_client = directory_client.create_file("IndianFoodDatasetCSV.csv")
    print("Open local file, prepare for uploading")
    
    file_contents = local_file.read()
    print("Uploading starts")
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))
    print("Uploading finish")


except Exception as ex:
    print('Exception:')
    print(ex)
