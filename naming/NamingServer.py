import concurrent
import queue
import sys
from random import random

from flask import Flask, request, json, make_response, jsonify
from threading import Thread, Condition, RLock
from threading import Event
import requests

from Structures import Registration, ClientHost, LockRequestQueue, ReplicaReport, FileLeaf, DirLockReport, FileLock
from collections import defaultdict

"""
=================
member variables
=================
"""
# system_root dictionary
system_root = dict()
"""
the system root, a diction object
"""
root_report = dict()
system_root[''] = dict()
"""
the root_report object, system_root[''] represents root dictionary

**example**: /directory/file , system_root[''] represents for the "/"

``system_root = {'':{'dirlock':DirLock()},'directory':{'files':[file], 'fileleaf':[FileLeaf(files)],'dirlock':DirLock()}}``
"""

# for register and globla storage
all_storageserver_files = set()
"""
a set of all files stored in this naming serverm
"""
storageserver_file_map = defaultdict(set)
"""
mapping between storageserver and files
"""
file_server_map = defaultdict(tuple)
"""
mapping between files and storageserver
"""
registered_storageserver = list()
"""
a list of registered storage servers in this naming server
"""

# for replica
replica_report = defaultdict(ReplicaReport)
"""
a dictionary of replica report storing ReplicaReport() objects

``replica_report= {"file1":ReplicaReport(file1),"file2":ReplicaReport(file2)......}``
"""

# for queueing and locking
lock_queue_report = LockRequestQueue()
"""
a structure of lock_queue_report for non-exclusive locks
"""

exclusive_wait_queue = list()
"""
a list of exclusive_wait_queue for exclusive locks
"""

LOCALHOST_IP = "127.0.0.1"
"""
IP for localhost
"""

FREQUENT = 10
"""
Magic number for replication
the acumulated access time a file has been visited. If the access time is over FREQUENT, ensure one replication
"""


"""
==================
start of functions
==================
"""

registration_api = Flask('registration_api')


@registration_api.route('/register', methods=['POST'])
def register_server():
    """
    **descripton**:the api for `/register`
    
    **returns**:
        If success, return a list of duplicate files to delete on the local storage of the registering storage server.

    **request**:
        1. *storage_ip*: storage server IP address.
        2. *client_port*: storage server port listening for the requests from client (aka storage port).
        3. *command_port*: storage server port listening for the requests from the naming server.
        4. *files*: list of files stored on the storage server.
                    This list is merged with the directory tree already present on the naming server.
                    Duplicate filenames are dropped.

    **ref**:
    https://stackoverflow.com/questions/20001229/how-to-get-posted-json-in-flask
    """
    request_content = request.json
    args = ["storage_ip","client_port","command_port","files"]

    if any(ele not in request_content for ele in args):
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "required arguments can not be None"
        }), 400)

    requested_storageserver = Registration(request_content["storage_ip"], request_content["client_port"],
                                           request_content["command_port"], request_content["files"])
    # check duplicate, return 409
    if requested_storageserver in registered_storageserver:
        return make_response(jsonify(
            {
                "exception_type": "IllegalStateException",
                "exception_info": "This storage client already registered."
            }), 409)

    # no duplicate, return duplicate files
    duplicate_files = find_duplicate_files(requested_storageserver)
    duplicate_files = add_files_and_storageservers(requested_storageserver, duplicate_files)
    return make_response(jsonify({
        "files": duplicate_files
    }), 200)


def find_duplicate_files(requested_storageserver):
    """
    **description**: Iterate all the files stored in the naming server, if the file in requested storage server already exists, add to duplicate file list

    **param**: requested_storageserver: the requested storageserver, containing storage_ip,client_port,command_port and files

    **return**: a list of duplicate files which already exist in the naming server
    """
    duplicate_files = []
    for file in requested_storageserver.files:
        if file in all_storageserver_files:
            duplicate_files.append(file)
    return duplicate_files


def construct_file_tree(files_to_construct):
    """
    **description** constructing a dictionary structure by a full path of file name
    
    **param** files_to_construct: the full path of a file

    **example**:
        ``directory1/directory1_1/file1``
        ``system_root{"directory1":{"directory1_1":{}}}``
    """
    for singlefile in files_to_construct:
        file_path = singlefile.split("/")
        dir = system_root
        for directory in file_path[:-1]:
            if directory not in dir:
                dir[directory] = dict()
            dir = dir[directory]


def to_parent_dir(filepath):
    """
    **description** : return to the parent directory of this filepath
    
    **param** : filepath: the full path of a file name
    
    **returns** turn to the parent directory

    **example**:
        directory1/directory1_1/file1
        return to system_root["system_root"]["directory1_1"]
    """
    file_path = filepath.split("/")
    dir = system_root
    for directory in file_path[:-1]:
        dir = dir[directory]
    return dir


def to_parent_path(filepath) -> str:
    """
    **description**: return to the string format parent directory of this filepath
    
    **param**: filepath: the full path of a file name
    
    **return**:  turn to the parent directory path

    **example**:
        directory1/directory1_1/file1, return to string "directory1/directory1_1"
    """
    file_path = filepath.split("/")
    file_dir_name = file_path[-1]
    lenname = len(file_dir_name)
    return filepath[:-1 - lenname]


def add_files_and_storageservers(requested_storageserver, duplicate_files):
    """
    **param**: requested_storageserver: the requesting storageserver, containing storage_ip,client_port,command_port and files
    
    **param**: duplicate_files: a list of duplicate files which already exist in the naming server
    
    **return**: updated list of duplicate files

    **details**:
    construct file dictionary tree, find duplicate files, add non-duplicated new files to
        1. add non-duplicated new files to it's own directory
        2. add non-duplicated new files to naming server all files
        3. add new file - storage server mapping
        4. add the requested storage server to naming server pool
        5. generate initial replica report for each file
    """
    # construct file dictionary tree
    # example: system_root = {"tmp":{"dist-systems-0"} for /tmp/dist-systems-0
    # update duplicate_files as arguments to keep track of the newest list and avoid repeatedly calculating file add list
    construct_file_tree(requested_storageserver.files)
    # add new files to it's directory
    duplicate_files = add_file_to_directory(requested_storageserver, duplicate_files)
    # add requested_storageserver to map
    duplicate_files = add_storageserver_to_map(requested_storageserver, duplicate_files)
    # add requested_storageserver to registered_storageserver set
    registered_storageserver.append(requested_storageserver)
    # generate initial replica report
    add_to_replica_report(requested_storageserver, duplicate_files)
    return duplicate_files


def add_file_to_directory(requested_storageserver, duplicate_files):
    """
    **param**: requested_storageserver: the requested storageserver, containing storage_ip,client_port,command_port and files

    **param**: duplicate_files: a list of duplicate files which already exist in the naming server

    **return**: updated list of duplicate files

    Iterate through files in requesting storage server, add new files to it's directory
        1. store short file name in dir["files"]
        2. store file lock structure in dir["fileleaf"]

    **structure example**:
        directory1/directory1_1/file1,
        system_root{"directory1": { "directory1_1" : {"files":[file1], "fileleaf":[FileLeaf(file1)]} } }
    """
    # dive into the parent root
    for singlefile in requested_storageserver.files:
        file_path = singlefile.split("/")
        filename = file_path[-1]
        dir = to_parent_dir(singlefile)

        # now in the file dir
        if filename in dir:
            duplicate_files.append(singlefile)
        else:
            # update or generate file structure
            # the nested dictionary has no default structure, thus the file existence have to be detected and generated
            if "files" in dir:
                dir["files"].append(filename)
                dir["fileleaf"].append(FileLeaf(filename))
            else:
                dir["files"] = list([filename])
                dir["fileleaf"] = list([FileLeaf(filename)])
    return duplicate_files


def add_storageserver_to_map(requested_storageserver, duplicate_files):
    """
    **param**: requested_storageserver: the requested storageserver, containing storage_ip,client_port,command_port and files
    
    **param**: duplicate_files: a list of duplicate files which already exist in the naming server
    
    **returns**: updated list of duplicate files

    **Implementation**:
        1. store ``requesting storage server - file`` mapping into naming server,
        2. store ``file - requesting storage server`` mapping into naming server

    **structure**:
        storageserver_file_map{requested_storageserver_keys : [list of files in this server]}
    """
    add_list = [file for file in requested_storageserver.files if (file not in duplicate_files)]
    # update all_storageserver_files set using union
    all_storageserver_files.update(set(add_list))

    # add to storageserver_file_map
    # add requested_storageserver.files and minus replica
    register_keypair = (requested_storageserver.storage_ip, requested_storageserver.client_port, requested_storageserver.command_port)
    storageserver_file_map[register_keypair].update(set(requested_storageserver.files).union(set(add_list)))

    # add to file-server map, which is the inverted one with storageserver_file_map
    for file in set(requested_storageserver.files).union(set(add_list)):
        if file not in file_server_map:
            file_server_map[file] = (requested_storageserver.storage_ip, requested_storageserver.client_port, requested_storageserver.command_port)

    return duplicate_files


def add_to_replica_report(requested_storageserver, duplicate_files):
    """
    **param**: requested_storageserver: the requested storageserver, containing storage_ip,client_port,command_port and files
    
    **param**: duplicate_files: a list of duplicate files which already exist in the naming server
    
    **return**: updated list of duplicate files

    **details**: generate initial replica report for files.The file are get by requested storage server files differentiate the duplicate files

    only files can be replicated, structure is  ``ReplicaReport``, which stores
        1. a list of storage server(AKA command_ports) which have this file replication
        2. time counts this file has been replicated
        3. the access time for this file, in order to decide replication or not
        4. the status of is_replicated or not

    """
    add_list = [file for file in requested_storageserver.files if (file not in duplicate_files)]
    for file in add_list:
        if file not in replica_report:
            # replica_report[file] = ReplicaReport()
            replica_report[file].command_ports = list()
            replica_report[file].replicaed_times = 0
            replica_report[file].visited_times = 1
            replica_report[file].is_replicated = False

        if requested_storageserver.command_port not in replica_report[file].command_ports:
            replica_report[file].command_ports.append(requested_storageserver.command_port)
            replica_report[file].replicaed_times += 1


"""
==================
service API
==================
"""

service_api = Flask('service_api')


@service_api.route('/is_valid_path', methods=['POST'])
def is_valid_path():
    """
    **description**: the api for `/is_valid_path`. Given a Path string, return whether it is a valid path.
    
    **return**: HTTP json response. `true` if the path is valid. `false` if the path is invalid. status code `200 OK`

    **validation**:
        1. The path string should be a sequence of components delimited with forward slashes.
        2. Empty components are dropped.
        3. The string must begin with a forward slash
        4. the string must not contain any colon character.
    """
    request_content = request.json

    if "path" not in request_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    checkpath = path_invalid(request_content['path'])
    if checkpath == "valid":
        return make_response(jsonify({"success": True}), 200)
    else:
        return make_response(jsonify({"success": False}), 404)



@service_api.route('/getstorage', methods=['POST'])
def get_storage():
    """
    **description**: api for `/getstorage`, Returns the IP and port info of the storage server that hosting the file.
    
    **return**: response_1 {"server_ip":server_ip,"server_port":server_port},200
    
    **exception** FileNotFoundException: If the file does not exist.
    
    **exception** IllegalArgumentException: If the file does not exist.

    
    """
    request_content = request.json

    if "path" not in request_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    requested_path = request_content["path"]
    stroage_ip, server_port, command_port = get_storage_map(requested_path)

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # FileNotFoundException
    args = [stroage_ip,server_port]
    if any(ele is None for ele in args):
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)
    else:
        return make_response(jsonify({
            "server_ip": stroage_ip,
            "server_port": server_port
        }), 200)


def empty_path_cleaner(path):
    """
    **description**: a fucntion to drop Empty path components dropped. such as ``//file``
    
    **param** path: full name of a file name
    
    **return** a list of cleaned path list containing each elements
    """
    path_list = path.strip().split("/")
    cleaner = [path_list[0]]
    cleaner.extend([x for x in path_list[1:] if x])
    return cleaner


@service_api.route('/list', methods=['POST'])
def list_contents():
    """
    **exception** FileNotFoundException: If the given path does not refer to a directory.
    
    **exception** IllegalArgumentException: If the given path is invalid.
    
    **return**: HTTP json response, {"files": file_list}), 200

    **description**: Lists the contents of a directory. The directory should be locked for shared access before this operation is
    performed, because this operation reads the directory's child list.
    """
    request_content = request.json

    if "path" not in request_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    requested_path = request_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    if requested_path == '/':
        is_root = True
        parent_dir = system_root
    else:
        is_root = False
        parent_dir = to_parent_path(requested_path)

    # list files and sub dirs in the content
    file_list = list_helper(requested_path)

    if file_list is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "given path does not refer to a directory."
        }), 400)

    return make_response(jsonify({"files": file_list}), 200)


def list_helper(path):
    """
    **descripton** the helper function to list files and sub dirctory name in a given path

    **param** path: the directory path to be visited

    **return**: a list file contents
    """
    is_root = False
    # the path should be a dirctory, not file
    is_dir = is_directory_helper(path)
    if not is_dir:
        return None

    path_list = empty_path_cleaner(path)
    if path == '/':
        is_root = True
        dir = system_root['']
        parent_dir = system_root
    else:
        # dive into dir
        dir = system_root
        for directory in path_list:
            if directory not in dir:
                return None
            dir = dir[directory]
            parent_dir = to_parent_path(path)

    # iterate files and sub directories in the directory
    list_contents = list()
    # append file
    if "files" in dir:
        list_contents.extend(dir["files"])
    # append subdirectory names
    sub_dir_names = [key for key in dir.keys() if key not in ["files", "fileleaf"]]
    list_contents.extend(sub_dir_names)

    return list_contents


@service_api.route('/is_directory', methods=['POST'])
def check_directory():
    """
    **descripton**: the api for `/is_directory`, Determines whether a path refers to a directory.

    **exception** FileNotFoundException: If the given path does not refer to a directory.

    **exception** IllegalArgumentException: If the given path is invalid.

    **return**: HTTP json response, {"success": success}), 200, `success` is True or False
    """
    requested_content = request.json

    if "path" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # root is a directory
    if requested_path == "/":
        return make_response(jsonify({
            "success": True}), 200)

    success = is_directory_helper(requested_content["path"])

    # FileNotFoundException
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)

    return make_response(jsonify({"success": success}), 200)


@service_api.route('/is_file', methods=['POST'])
def check_file():
    """
    **descripton**: the api for `/is_file`, Determines whether a path refers to a directory.

    **exception** FileNotFoundException: If the given path does not refer to a directory.

    **exception** IllegalArgumentException: If the given path is invalid.

    **return**: HTTP json response, {"success": success}), 200, `success` is True or False
    """
    requested_content = request.json

    if "path" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # root is not a file
    if requested_path == "/":
        return make_response(jsonify({
            "success": False}), 200)

    # `True` if is a directory, `false` if not, `None` if not found
    success = is_file_helper(requested_content["path"])

    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)

    return make_response(jsonify({"success": success}), 200)


def is_directory(dir_name, directory):
    """
    **descripton**: the judgement to decide whether a path is a directory

    **param** dir_name: name of a directory

    **param** directory: the directory to be checked

    **return**: `True` if is a directory, `false` if not, `None` if not found
    """
    if dir_name in directory:
        return True
    if "files" in directory and dir_name in directory["files"]:
        return False
    return None


def is_file(file_name, direcory):
    """
    **descripton**: the judgement to decide whether a path is a file

    **param** file_name: name of a file

    **param** direcory: the directory to be checked

    **return**: `True` if is a directory
    """
    if "files" in direcory and file_name in direcory["files"]:
        return True


def is_file_helper(path):
    """
    **descripton**: the helper function to decide whether a path is a valid file

    **param** path: the full name of a file

    **return**: `True` if is a directory, `False` if not
    """
    path_list = empty_path_cleaner(path)
    file_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if (directory not in root) and not (directory == ''):
            return None
        root = root[directory]

    if "files" in root and file_name in root["files"]:
        return True
    return False


def is_directory_helper(path):
    """
    **descripton**: the helper function to decide whether a path is a valid directory

    **param** path: the full name of a directory

    **return**: `True` if is a directory, `False` if not
    """
    path_list = empty_path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if (directory not in root) and not (directory == ''):
            return None
        root = root[directory]

    return is_directory(dir_name, root)


@service_api.route('/create_directory', methods=['POST'])
def create_directory():
    """
    **descripton**: Creates the given directory, if it does not exist. Path at which the directory is to be created.

    **exception** FileNotFoundException: If the given path does not refer to a directory.

    **exception** IllegalArgumentException: If the given path is invalid.

    **return**: HTTP json response, {"success": success}), 200, `success` is True or False
    """
    requested_content = request.json

    if "path" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    dir_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    # should not create root
    if dir_path == "/":
        return make_response(jsonify({"success": False}), 200)

    success = create_directory_helper(requested_content["path"])

    # FileNotFoundException
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "parent directory does not exist."
        }), 400)

    return make_response(jsonify({"success": success}), 200)


def create_directory_helper(path):
    """
    **descripton**: the helper function to create a directory

    **param** path: the full name of a dir path to be created

    **return**: `True` if created successfully, `False` if not, None if path not found
    """
    # if is root or not a directory
    if path == "/" and not is_directory_helper(path):
        return False

    path_list = empty_path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if directory not in root:
            return None
        root = root[directory]

    # dir already exists or dirname is a file
    if (dir_name in root) or ("files" in root and dir_name in root["files"]):
        return False

    # create directory
    root[dir_name] = dict()
    return True



@service_api.route('/create_file', methods=['POST'])
def create_file():
    """
    **descripton**: api for `/create_file`, Creates the given file, if it does not exist.

    **exception** FileNotFoundException: If the given path does not refer to a directory.

    **exception** IllegalArgumentException: If the given path is invalid.

    **return**: HTTP json response, {"success": success}), 200, `success` is True or False
    """
    requested_content = request.json

    if "path" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)

    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # root is not a file
    if requested_path == "/":
        return make_response(jsonify({"success": False}), 200)

    success = create_file_helper(requested_content["path"])

    # FileNotFoundException
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "parent directory does not exist."
        }), 400)

    # add file to all_storageserver_files
    if success:
        all_storageserver_files.add(requested_content["path"])

    return make_response(jsonify({"success": success}), 200)


def create_file_helper(path):
    """
    **descripton**: helper function to create a file, call storage_create by sending request.
    if success, add the file to all file set and initiale replica report for the file

    **param** path: the full path name

    **return**: `True` if success, else `False`
    """
    path_list = empty_path_cleaner(path)
    file_name = path_list[-1]

    if path == "/":
        root = system_root[""]

    root = system_root
    for directory in path_list[:-1]:
        if directory not in root:
            if directory == '':
                root[''] = dict()
            else:
                return None
        root = root[directory]

    # file already exists
    if (file_name in root) or ("files" in root and file_name in root["files"]):
        return False

    # send request to "http://localhost:command_port/storage_create"
    # just choose one server
    command_port = str(registered_storageserver[0].command_port)
    response = json.loads(
        requests.post("http://localhost:" + command_port + "/storage_create",
                      json={"path": path}).text
    )

    success = response["success"]
    keys = (registered_storageserver[0].storage_ip, registered_storageserver[0].client_port, registered_storageserver[0].command_port)

    # if success create file
    if success:
        if "files" not in root:
            root["files"] = [file_name]
            root["fileleaf"] = list([FileLeaf(file_name)])
        else:
            root["files"].append(file_name)
            root["fileleaf"].append(FileLeaf(file_name))
        # add file to all file set and initiate replica report
        storageserver_file_map[keys].add(path)
        replica_report[path] = ReplicaReport()

    return success


def get_storage_map(path_to_find):
    """
    **descripton**: helper function to find the storageserver which have stored the file

    **param** path_to_find: the file name to be found

    **return**: (storage_ip,client_port,command_port) if exists, (None,None,None) if not found
    """
    for storageserver in storageserver_file_map:
        if path_to_find in storageserver_file_map[storageserver]:
            return storageserver
    return None, None, None


def get_filestorage_map(path_to_find):
    """
    **descripton**: helper function to find the storageserver which have stored the file

    **param** path_to_find: the file name to be found

    **return**: (storage_ip,client_port,command_port) if exists, (None,None,None) if not found
    """
    if path_to_find in file_server_map:
        return file_server_map[path_to_find]
    return None, None, None


def path_invalid(path):
    """
    **descripton**: the validation functon for a path, the path should start with '/' and with no ':' inside

    **param** path: the path to be validated

    **return**: 'valid' if valid, else HTTP Exception response
    """
    if not path or len(path) == 0:
        return {
                   "exception_type": "IllegalArgumentException",
                   "exception_info": "path can not be None"
               }, 400
    if not (path[0] == '/') or ':' in path:
        return {
                   "exception_type": "IllegalArgumentException",
                   "exception_info": "path has invalid format"
               }, 400
    return "valid"


"""
==================
checkpoint
==================
"""

@service_api.route('/lock', methods=['POST'])
def lock_path():
    """
    **descripton**: api for `/lock`. Creates the given file, if it does not exist.
    Locks a file or directory for either shared or exclusive access. Locking a file for shared access is considered
    by the naming server to be a read request, and may cause the file to be replicated. Locking a file for exclusive
    access is considered to be a write request, and causes all copies of the file but one to be deleted(invalidation)

    **exception** FileNotFoundException: If the given path does not refer to a directory.

    **exception** IllegalArgumentException: If the given path is invalid.

    **return**: HTTP json response : empty, 200

    **Strueture**:
    ``
        lock_queue_report{
            shared_counter = 0, the counter of shared lock
            queue = list() , the list of current queue, include all exclusive and shared lock request
            queue_size = 0 , the length of the current queue
        }
    ``
    """
    requested_content = request.json

    if "path" not in requested_content or "exclusive" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "Required Arguements can not be None"
        }), 400)

    requested_path = requested_content["path"]
    exclusive = requested_content["exclusive"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    is_dir = is_directory_helper(requested_path)
    is_file = is_file_helper(requested_path)

    # FileNotFoundException
    if (is_file or (not is_dir)) and (requested_path not in all_storageserver_files):
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "path cannot be found."
        }), 400)

    if requested_path == "/":
        # lock root
        return lock_root_operation(exclusive)
    else:
        if is_dir:
            # lock dirctory
            return lock_directory_operation(requested_path, exclusive)
        else:
            # lock file
            return lock_file_operation(requested_path, exclusive)


def lock_root_operation(exclusive=True):
    """
    **descripton**: the helper function to lock the root

    **param** exclusive: the lock is exclusive or not. exclusive if is write , shared if is read

    **return**: HTTP json response : empty, 200
    """
    requested_path = "/"
    filelock, can_lock = acquire_lock_and_ability(is_root=True, exclusive_lock=exclusive)

    if not filelock:  # never be locked
        if can_lock:
            add_to_shared_queue(exclusive)
    else:
        # has locking report
        can_direct_lock = (lock_queue_report.queue_size == 0 and can_lock)

        # no need for queue
        if can_direct_lock:
            add_to_shared_queue(exclusive)

        # queueing design
        # suppose users `A` and `B` both currently hold the lock with shared access. User `C` arrives
        # and requests exclusive access. > User `C` is then placed in a queue. If another user, `D`, arrives and
        # requests shared access, he is not permitted to take the lock immediately, > even though it is currently
        # taken by `A` and `B` for shared access. User `D` must wait until `C` is done with the lock.
        if not can_direct_lock:  # lock_queue_report.queue_size > 0 or not can_lock
            if exclusive and lock_queue_report.shared_counter > 0:  # can not operate exclusive lock, should queue
                lock_request = (filelock, requested_path, exclusive)
                # D have to wait for C to complete, queuing up D
                exclusive_wait_queue.append(lock_request)
            else:
                # the lock is shared // the lock is exclusive but there are no previous exlusive lock request queueing
                # can operate exclusive lock
                # example, locking / for exclusive access and / for shared access
                if len(exclusive_wait_queue) == lock_queue_report.queue_size:
                    # all previous lock is oppcupied and queuing,
                    # generate a new lock for this new request, generating a new operation lock
                    filelock = Event()
                elif lock_queue_report.queue:  # operate the queueing request first
                    filelock = lock_queue_report.queue[-1][0]
                lock_request = (filelock, requested_path, exclusive)

            # append to global queue and wait
            lock_queue_report.queue.append(lock_request)
            lock_queue_report.queue_size += 1
            # wait
            lock_request[0].wait()
            # operate the first request
            exclusive = lock_queue_report.queue.pop(0)[2]
            lock_queue_report.queue_size -= 1
            # if is not exclusive and queue_size == 0, shared_counter increment by 1
            add_to_shared_queue(exclusive)

    # do lock
    success = do_lock(is_root=True, exclusive_lock=exclusive)
    # return empty ,200 if success
    content = "" if success else "Lock Failed"
    return make_response(content, 200)


def add_to_shared_queue(exclusive):
    """
    **descripton**: only if the lock is shared and no lock request is queuing can we increment the shared counter by one.

    **param** exclusive: the lock is exclusive or not

    **return**: None
    """
    if not exclusive and lock_queue_report.queue_size == 0:
        lock_queue_report.shared_counter += 1


def lock_directory_operation(path, exclusive):
    """
    **descripton**: the helper function to lock directory
    the dirctory self lock report should be updated, no file to manipulate

    **param** path: the directory full path

    **param** exclusive: the lock is exclusive or not

    **return**: HTTP json response : empty, 200
    """
    success_add = add_lock_request(path=path, exclusive_lock=exclusive)
    # FileNotFoundException
    if not success_add:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "directory cannot be found."
        }), 400)

    success_lock = do_lock(path=path, exclusive_lock=exclusive)
    content = "" if success_lock else "Lock Failed"
    return make_response(content, 200)


def lock_file_operation(path, exclusive):
    """
    **descripton**: helper function to lock a single file, manipulate file, thus should consider queue

    **param** path: the file full path

    **param** exclusive: the lock is exclusive or not

    **return**: HTTP json response : empty, 200
    """
    success_add = add_lock_request(path=path, exclusive_lock=exclusive)

    # FileNotFoundException
    if not success_add:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "directory cannot be found."
        }), 400)

    # first come first server
    if lock_queue_report.queue_size > 0:
        queued_lock_request = lock_queue_report.queue.pop(0)
        lock_queue_report.queue_size -= 1
        path, exclusive = queued_lock_request[1], queued_lock_request[2]

    success = do_lock(path=path, exclusive_lock=exclusive)
    content = "" if success else "Lock Failed"
    return make_response(content, 200)


def add_lock_request(path, exclusive_lock):
    """
    **descripton**: add the lock request to the global queue
    if the path can not be locked, return False
    if the path can be locked, but can acquire `exclusive_lock` , add the lock_request to queue to
    if the path can be locked, but can not acquire `exclusive_lock`,nothing to add

    **param** path: the file full path

    **param** exclusive_lock: the lock is exclusive or not

    **return**: True if successful add, False if not
    """
    filelock, can_lock = acquire_lock_and_ability(path=path, exclusive_lock=exclusive_lock)

    # can not acquire lock of `exclusive_lock` from path `path` and can not lock, return False
    if filelock is None and not can_lock:
        return False

    can_direct_lock = (lock_queue_report.queue_size == 0 and can_lock)
    if can_direct_lock:
        return True

    # can not direct lock
    if not can_direct_lock and filelock:
        # if filelock:
        # can acquire required lock from path, but can not direct lock, add to queue
        lock_request = (filelock, path, exclusive_lock)
        lock_queue_report.queue.append(lock_request)
        lock_queue_report.queue_size += 1
        lock_request[0].wait()
    # can direct lock
    return True


def check_upper_dir_locker(path):
    """
    **descripton**: An object can be considered to be **effectively locked** for exclusive access
    if one of the directories on the path to it is already locked for exclusive access:

    **param** path: the file name to iterate from upper down

    **return**: the filelock, the ability to be lock
    """
    path_list = path.split("/")
    parent_dir = system_root
    for dir_name in path_list[:-1]:
        current_dir = parent_dir[dir_name]
        if ("dirlock" in current_dir) and current_dir["dirlock"].exclusive:
            return current_dir["dirlock"].filelock, False

        parent_dir = parent_dir[dir_name]
    return None, True


def dir_in_parent_directory(dir_name, parent_dir):
    """
    **descripton**: check whether the dirctory belongs to another directory

    **param** dir_name: the short directory name

    **param** parent_dir: the parent dirctory name

    **return**: True if is sub directory, False if not
    """
    if dir_name in parent_dir:
        return True
    return False


def dir_lock_and_ability_helper(is_root=False, directory=None, dir_name=None, exclusive_lock=True):
    """
    **descripton**: check if a directory can be locked

    **param** is_root: is root or not, default False

    **param** directory: the full directory name to be checked

    **param** dir_name: the short sub directory name

    **param** exclusive_lock: the lock is exclusive or shared

    **return**:`None,true` or `lock, False`
    """
    # if dir_name not a subdir of directory, return False
    if not is_root and not dir_in_parent_directory(dir_name, directory):
        return None, False
    # get the root report, which is stored in
    # root: system_root['']
    # normal directory: directory[dir_name] = {"files":[], "fileleaf":[], "dirlock":[]}
    lock_report = system_root[''] if is_root else directory[dir_name]

    # no locking record, can be locked
    if not lock_report or "dirlock" not in lock_report:
        return None, True

    # has locking record, current lock is exclusive or required lock is exclusive
    if lock_report["dirlock"].locked and (lock_report["dirlock"].exclusive or exclusive_lock):
        return lock_report["dirlock"].filelock, False

    # has locking record, current lock and required lock are both shared
    return (lock_report["dirlock"].filelock if is_root else None), True


def file_in_parent_directory(file_name, parent_dir):
    """
    **descripton**: check whether the dirctory belongs to another directory

    **param** file_name: the short file name

    **param** parent_dir: the parent dirctory name

    **return**: True if is sub directory, False if not
    """
    if "fileleaf" not in parent_dir:
        return False

    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            return True
    return False


def file_lock_and_ability_helper(parent_dir=None, file_name=None, exclusive_lock=True):
    """
    **descripton**: check if a file can be locked

    **param** parent_dir: the parent directory

    **param** file_name: the short file name

    **param** exclusive_lock: the lock type, is exclusive or shared

    **return**: (None,True), if can be locked. (The lock, False), if can't
    """
    if not file_in_parent_directory(file_name, parent_dir):
        return None, False

    if "fileleaf" not in parent_dir:
        return None, True
    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            if file.locked:
                if file.exclusive or exclusive_lock:
                    return file.filelock, False
                else:
                    return None, True
            else:
                return None, True



def acquire_lock_and_ability(is_root=False, path=None, exclusive_lock=True):
    """
    **descripton**: whether the path can acquire a exclusive/share lock

    **param** is_root: is root or not

    **param** path: the full path name

    **param** exclusive_lock: the lock is exclusive or not

    **return**: **return**: (None,True), if can be locked. (The lock, False), if can't
    """
    if is_root:
        return dir_lock_and_ability_helper(is_root=True, exclusive_lock=exclusive_lock)
    else:
        # check if upper directories can be locked
        lock, ablility = check_upper_dir_locker(path)
        # the upper dirctionaries can not be locked, stop at where is locked
        if not ablility:
            return lock, ablility

        # get lock abilities according to directory or file
        path_list = path.split("/")
        file_or_dir_name = path_list[-1]
        is_dir = is_directory_helper(path)
        parent_dir = to_parent_dir(path)
        if is_dir:
            return dir_lock_and_ability_helper(directory=parent_dir, dir_name=file_or_dir_name,
                                               exclusive_lock=exclusive_lock)
        else:  # is file
            return file_lock_and_ability_helper(parent_dir=parent_dir, file_name=file_or_dir_name,
                                                exclusive_lock=exclusive_lock)


def do_lock(is_root=False, path=None, exclusive_lock=True):
    """
    **descripton**:
    lock root, directory, file according to the path
        1. if is root, manipulate in system_root['']
        2. if is a directory, lock all upper directory, acquire lock in directory["dirLock"]
        3. if is a file, lock all upper directory, acquire file lock in directory["fileleaf"], update replication

    **Replication update schema**:
        1. if the lock is exclusive, delete all replica and leave only one
        2. if the lock is shared, detect file access time,if have been accessed for certain times
           (which is set in magic number FREQUENT)
           check and replicate this file(if not replicated)

    **param** is_root: is root or not

    **param** path: the dirctory or file full path

    **param** exclusive_lock: the required lock is exclusive or not

    **return**: True if do lock successfully
    """
    filelock, can_lock = acquire_lock_and_ability(is_root, path, exclusive_lock)
    if not can_lock:
        return False

    if path == "/":
        is_root = True

    # lock root
    if is_root:
        # enter root directory
        root_report = system_root['']

        # if the root
        #   1. have no lock report or is not locked
        #   2. is locked but not exclusive
        # we can acquire lock in this root
        if "dirlock" not in root_report:
            root_report["dirlock"] = DirLockReport()
            root_report["dirlock"].acquire(exclusive=exclusive_lock)
        elif (not root_report["dirlock"].locked) or (
                root_report["dirlock"].locked and not root_report["dirlock"].exclusive):
            root_report["dirlock"].acquire(exclusive=exclusive_lock)
        return True
    else:
        is_dir = is_directory_helper(path)
        path_list = path.split("/")
        dir_or_file_name = path_list[-1]
        parent_dir = to_parent_dir(path)

        # first lock dirs from up to bottom
        lock_upper_dir(path)

        if is_dir and dir_in_parent_directory(dir_or_file_name, parent_dir):
            # lock a directory
            return lock_dirctory(dir_or_file_name, parent_dir, exclusive_lock)
        elif file_in_parent_directory(dir_or_file_name, parent_dir):
            # lock a file
            # and update file replication here
            return lock_file(dir_or_file_name, parent_dir, exclusive_lock, path)


def lock_upper_dir(path):
    """
    **descripton**: lock all the upper directories from root to parent dirctory according to the given path

    **param** path: the full name of the requested path

    **return**: None
    """
    path_list = path.split("/")
    parent_dir = system_root

    for dir_name in path_list[:-1]:
        child_dir = parent_dir[dir_name]
        if "dirlock" not in child_dir:
            child_dir["dirlock"] = DirLockReport()
        child_dir["dirlock"].acquire(exclusive=False)
        parent_dir = parent_dir[dir_name]


def unlock_upper_dir(path):
    """
    **descripton**: unlock all the upper dirctorys from root to parent dirctory according to the given path

    **param** path: the full name of the requested path

    **return**: None
    """
    path_list = path.split("/")
    parent_dir = system_root

    for dir_name in path_list[:-1]:
        child_dir = parent_dir[dir_name]
        if "dirlock" not in child_dir:
            child_dir["dirlock"] = DirLockReport()
        child_dir["dirlock"].release()
        parent_dir = parent_dir[dir_name]


def lock_dirctory(dir_name, parent_dir, exclusive_lock):
    """
    **descripton**: lock a specific directory in parent dirctory by acquiring exclusive_lock

    **param** dir_name: the short directory name

    **param** parent_dir: the parent directory

    **param** exclusive_lock: lock is exclusive or not

    **return**: True if success
    """
    target_dir = parent_dir[dir_name]
    if "dirlock" not in target_dir:
        target_dir["dirlock"] = DirLockReport()
    if not target_dir["dirlock"].locked:
        target_dir["dirlock"].acquire(exclusive=exclusive_lock)
    return True


def lock_file(file_name, parent_dir, exclusive_lock, path):
    """
    **descripton**: lock a specific file in parent dirctory by acquiring exclusive_lock
    delete all replication except for one instance if the lock is exclusive
    add replication if the file is shared and have been accessed for FREQUENT times

    **Replication update schema**:
        1. if the lock is exclusive, delete all replica and leave only one
        2. if the lock is shared, detect file access time,if have been accessed for certain times
           (which is set in magic number FREQUENT)
           check and replicate this file(if not replicated)

    **param** file_name: the short directory name

    **param** parent_dir: the parent directory

    **param** exclusive_lock: lock is exclusive or not

    **return**: True if success, False if not
    """
    if "fileleaf" not in parent_dir:
        return False

    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            if not file.locked:
                file.acquire(exclusive_lock)
                parent_dir["replica_report"] = replica_report[path]

                # the replcate schema
                replica_success = add_or_delete_replica(path, exclusive_lock)
            return True
    return False


def should_start_new_replication(path, exclusive_lock):
    """
    **descripton**: judgement whether the path acquiring a lock need to start a new replication process.

    **requirements**:
        1. the lock should not be exclusive(AKA write)
        2. the current path is not yet replicated
        3. the access time is more than a threshold(set by the magic number FREQUENT here)

    **param** path: the full path of a file

    **param** exclusive_lock: the lock is replicated or not

    **return**: `True` if should start a new replication, `False` if should not
    """
    return (not exclusive_lock) and \
           (not replica_report[path].is_replicated) and \
           (replica_report[path].visited_times >= FREQUENT)


def should_delete_replication(path, exclusive_lock):
    """
    **descripton**: judgement whether we should invalidate all stale replication for the path acquiring a lock should invalidate

    **requirements**:
        1. the lock should be exclusive(AKA write)
        2. the current path has already be somewhat replicated

    **param** path: the full path of a file

    **param** exclusive_lock: the lock is replicated or not

    **return**: `True` if should start a new replication, `False` if should not
    """
    return (exclusive_lock) and \
           (replica_report[path].is_replicated)


def add_or_delete_replica(path, exclusive_lock):
    """
    **descripton**: if the path acquiring a shared lock should be replicated, start a new process of replication.
    if the stale replication of path acquiring a exclusive lock should be invalidate, start a new process of delete

    **param** path: the full path of a file

    **param** exclusive_lock: the lock is replicated or not

    **return**: `True` if operation success, `False` if failed
    """
    if exclusive_lock:
        if should_delete_replication(path, exclusive_lock):
            # delete stale replication of this file
            return delete_exclusive_replica(path)
    else:  # shared lock
        replica_report[path].visited_times += 1
        if should_start_new_replication(path, exclusive_lock):
            # make a new replication
            return copy_from_storageserver(path)


def copy_from_storageserver(path):
    """
    **descripton**: replicate a file every FREQUENT times of visit.

    **param** path: the full path of a file

    **return**: `True` if operation success, `False` if failed
    """
    # set access counter to 1, do replication every FREQUENT times of visit
    replica_report[path].visited_times = 1

    # find the current hosting server
    current_stroage_ip, current_server_port, current_command_port = get_storage_map(path)
    # the storage server to be requested should be the one holding the file
    current_server = Registration(current_stroage_ip, current_server_port, current_command_port)

    # find a differnt server to copy and replicate the request file
    for candidate_server in registered_storageserver:
        if current_server.is_different_server(candidate_server):
            new_server_command_port = candidate_server.command_port

    # the candidate server to be requested now is:
    # candidate_server = Registration(current_stroage_ip, current_server_port, new_server_command_port)

    # check validation
    if any(elem is None for elem in [path, LOCALHOST_IP, current_server_port, new_server_command_port]):
        return False

    # start a new process of replication
    Thread(target=replica_thread,
           args=(path, LOCALHOST_IP, current_server_port, new_server_command_port)).start()
    return True


def replica_thread(path, server_ip, server_port, command_port):
    """
    **descripton**: Ask for a thread that replication file.

    **Update replica_report schema**:
    if the HTTP response is successful,
        1. update replica_report[path].is_replicated
        2. add command_port into list of replication servers.(can be duplicate, indicating numbers of replication)
        3. replicaed_times increment by 1

    **param** path: Path to the file to be copied.

    **param** server_ip: IP of the storage server that hosting the file , should be "127.0.0.1"

    **param** server_port: storage port of the storage server that hosting the file.

    **param** command_port: storage port of the storage server that receiving the `/copy` command.

    **return**: None
    """
    is_success = send_replica_request(path, server_ip, server_port, command_port)
    #
    if is_success:
        replica_report[path].is_replicated = is_success
        replica_report[path].command_ports.append(command_port)
        replica_report[path].replicaed_times += 1


def send_replica_request(path, server_ip, server_port, command_port):
    """
    **descripton**: send `/storage_copy` request to http://localhost:command_port/storage_copy

    **param** path: Path to the file to be copied.

    **param** server_ip: IP of the storage server that hosting the file , should be "127.0.0.1"

    **param** server_port: storage port of the storage server that hosting the file.

    **param** command_port: storage port of the storage server that receiving the `/copy` command.

    **return**: `True` if the HTTP reponse successfully, `False` if not.
    """
    request = {
        "path": path,
        "server_ip": server_ip,
        "server_port": server_port,
    }
    response = json.loads(
        requests.post("http://localhost:" + str(command_port) + "/storage_copy",
                      json=request).text
    )
    is_success = response["success"]
    return is_success


def delete_exclusive_replica(path):
    """
    **descripton**: Ask for a thread that delete file

    **param** path: Path to the file to be copied.

    **return**: True if success
    """
    if replica_report[path].replicaed_times > 1:
        # delete from the last, convenient for pop()
        command_port = replica_report[path].command_ports[-1]

        if any(elem is None for elem in [path, command_port]):
            return False

        # start a new process of delete
        Thread(target=delete_thread,
               args=(path, command_port)).start()
    return True


def delete_thread(path, command_port):
    """
    **descripton**: ask for delete a file

    **Update replica_report**:
        if the HTTP response is successful,
            1. update replica_report[path].is_replicated
            2. delete the command that have been deleted
            3. replicated_times decremented by 1

    **param** path: to be deleted

    **param** command_port: the server command port

    **return**: None
    """
    is_success = send_delete_request(path, command_port)
    # update replica_report
    if is_success:
        replica_report[path].is_replicated = False
        replica_report[path].command_ports.pop()
        replica_report[path].replicaed_times -= 1


def send_delete_request(path, command_port):
    """
    **descripton**: send `/storage_delete` request to http://localhost:command_port/storage_delete

    **param** path: to be deleted

    **param** command_port: the server command port

    **return**: `True` if success, `False` if not
    """
    request = {"path": path}
    response = json.loads(
        requests.post("http://localhost:" + str(command_port) + "/storage_delete",
                      json=request)
    )
    is_success = response["success"]
    return is_success



def do_unlock(is_root=False, path=None):
    """
    **descripton**:
     unlock a root, directory or file
        1. if is root: release lock by priority: exclusive_wait_queue > lock_queue_report.queue > root_report["dirlock"]
        2. if is directory: release lock from upper to bottom in root_report["dirlock"]
        3. if is file: release lock from upper to bottom directory and in parent_dir["fileleaf"]

    **param** is_root: is root or not

    **param** path: the path full name to be unlocked

    **return**: True if successfully unlock
    """
    if is_root:
        root_report = system_root['']
        if "dirlock" not in root_report:
            return True
        # the lock is not exclusive, deduce the reading lock by 1
        if not root_report["dirlock"].exclusive:
            lock_queue_report.shared_counter -= 1
        # the lock is not exclusive, deduce the reading lock by 1
        # now able to release lock
        if lock_queue_report.shared_counter == 0:
            # exclusive lock queue first
            if exclusive_wait_queue:
                filelock, dir_path, exclusive = exclusive_wait_queue.pop(0)
            else:
                if lock_queue_report.queue:
                    filelock, dir_path, exclusive = lock_queue_report.queue[0]
                else:
                    filelock = root_report["dirlock"].filelock
            filelock.set()
            root_report["dirlock"].set_status(locked=False, exclusive=False)
        return True
    else:
        # unlock directory or file
        is_dir = is_directory_helper(path)
        path_list = path.split("/")
        dir_or_file_name = path_list[-1]
        unlock_upper_dir(path)
        parent_dir = to_parent_dir(path)
        # unlock dirctory
        if is_dir and dir_in_parent_directory(dir_or_file_name, parent_dir):
            return unlock_dirctory(dir_or_file_name, parent_dir)
        else:
            return unlock_file(dir_or_file_name, parent_dir)


def unlock_upper_dir(path):
    """
    **descripton**: unlock all upper directories

    **param** path: the full path name

    **return**: True if unlock successfully, else False
    """
    path_list = path.split("/")
    parent_dir = system_root
    for dir_name in path_list[:-1]:
        parent_dir = parent_dir[dir_name]
        if "dirlock" in parent_dir and parent_dir["dirlock"].locked:
            parent_dir["dirlock"].release()
    return True


def unlock_dirctory(dir_name, parent_dir):
    """
    **descripton**: unlock specific directory in a parent directory

    **param** dir_name: the short name for dirctory

    **param** parent_dir: the parent directory

    **return**: True if unlock successfully, else False
    """
    if not dir_in_parent_directory(dir_name, parent_dir):
        return False

    target_dir = parent_dir[dir_name]
    if "dirlock" in target_dir and target_dir["dirlock"].locked:
        target_dir["dirlock"].release()
    return True


def unlock_file(file_name, parent_dir):
    """
    **descripton**: unlock specific file in a parent directory

    **param** file_name: the short name for file

    **param** parent_dir: the parent directory

    **return**: True if unlock successfully, else False
    """
    if not file_in_parent_directory(file_name, parent_dir):
        return False

    if "fileleaf" not in parent_dir:
        return True

    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            if file.locked:
                file.release()
    return True


@service_api.route('/unlock', methods=['POST'])
def unlock_path():
    """
    **descripton**: Unlocks a file or directory.

    **return**: empty (If unlock successfully, the response body should be empty.), 200

    **exception** IllegalArgumentException:
        If the path is invalid or cannot be found.
        This is a client programming error, as the path must have previously been locked,
        and cannot be removed while it is locked.
    """
    requested_content = request.json

    if "path" not in requested_content:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "Path can not be None"
        }), 400)

    dir_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    is_dir = is_directory_helper(dir_path)
    is_file = is_file_helper(dir_path)

    # IllegalArgumentException
    if (dir_path not in all_storageserver_files) and (is_file or not is_dir):
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path cannot be found."
        }), 400)

    if dir_path == "/":
        success = do_unlock(is_root=True)
    else:
        success = do_unlock(is_root=False, path=dir_path)
    content = "" if success else "Unlock Failed"
    return make_response(content, 200)


@service_api.route('/delete', methods=['POST'])
def delete_dir_or_file():
    """
    **Description**: Deletes a file or directory.
        1. if is a file, remove it from all_storageserver_files,
           remove all structures about it("files","fileleaf",replica_report[file]),
        2. if is a directory, remove all files under it and delete the directory.

    **return**:
        `true` if the file or directory is successfully deleted, return `false` otherwise.
        The root directory cannot be deleted.

    **exception** FileNotFoundException: If the file or parent directory does not exist.

    **exception** IllegalArgumentException: If the given path is invalid.
    """
    requested_content = request.json

    if requested_content["path"] is None:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "required arguments missing."
        }), 400)

    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    is_dir = is_directory_helper(requested_path)
    is_file = is_file_helper(requested_path)

    # can not delete root directory
    if requested_path == "/" or requested_path == "":
        return make_response(jsonify({"success": False}), 200)

    # FileNotFoundException
    if (requested_path not in all_storageserver_files) and (is_file or not is_dir):
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "path cannot be found."
        }), 400)

    parent_path = to_parent_path(requested_path)
    parent_dir = to_parent_dir(requested_path)
    file_or_dir_name = requested_path.split("/")[-1]
    is_root = True if (parent_path == "") else False
    file_to_delete = set()

    if is_dir:
        # delete a diectory
        for file in all_storageserver_files:
            # find all files with the directory prefix
            if file.find(requested_path) != -1:
                do_lock(is_root=is_root, path=parent_path, exclusive_lock=True)
                delete_given_path(file, requested_path)
                do_unlock(is_root=is_root, path=parent_path)
                file_to_delete.add(file)
        # remove all files under directory, clear the directory
        all_storageserver_files.difference(file_to_delete)
        del parent_dir[file_or_dir_name]
    else:
        # delete a file
        do_lock(is_root=is_root, path=parent_path, exclusive_lock=True)
        delete_given_path(requested_path, requested_path)
        do_unlock(is_root=is_root, path=parent_path)
        # delete the file
        completely_delete_file(requested_path)

    return make_response(jsonify({"success": True}), 200)


def delete_given_path(path, file_or_dir):
    """
    **descripton**: delete a file or directory by calling a /storage_delete HTTP request

    **param** path: the filename to be deleted
                 if is a file, the same as file_or_dir, if is a directory, have the same directory as file_or_dir)

    **param** file_or_dir: the name of the file or dir

    **return**: None
    """
    for command_port in replica_report[path].command_ports:
        send_deletion_request(file_or_dir, command_port)
    # clear replica_report by initialize it
    replica_report[path] = ReplicaReport()
    return


def completely_delete_file(filepath):
    """
    **descripton**: completely remove all file related structures, including parent_dir["files","fileleaf"] and all_storageserver_files

    **param** filepath: the full name of the file to be deleted in name server

    **return**: None
    """
    file_list = filepath.split("/")
    file_name = file_list[-1]
    parent_dir = to_parent_dir(filepath)

    if "files" in parent_dir and file_name in parent_dir["files"]:
        parent_dir["files"].remove(file_name)
        for file in parent_dir["fileleaf"]:
            if file.file_name == file_name:
                parent_dir["fileleaf"].remove(file)

    all_storageserver_files.remove(filepath)


def send_deletion_request(path, command_port):
    """
    **descripton**: a HTTP request to call `/storage_delete`

    **param** path: the path to be deleted, can be a file or a directory

    **param** command_port: the command port for the storage server

    **return**: the success information from HTTP response
    """
    response = json.loads(
        requests.post("http://localhost:" + str(command_port) + "/storage_delete",
                      json={"path": path}).text
    )
    return response["success"]


def start_registration(registration_port):
    """
    **descripton**: the Flask run function for @registration_api

    **param** registration_port: the port for Naming Server Registration

    **return**: None
    """
    registration_api.run(host='localhost', port=int(registration_port))


def start_service(service_port):
    """
    **descripton**: the Flask run function for @service_api

    **param** service_port: the port for Naming Server Service

    **return**: None
    """
    service_api.run(host='localhost', port=int(service_port))


if __name__ == '__main__':
    service_port = sys.argv[1]
    registration_port = sys.argv[2]
    Thread(target=start_registration, args=(registration_port,)).start()
    start_service(service_port)
