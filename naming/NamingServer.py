from flask import Flask, request, json, make_response, jsonify
from threading import Thread
from threading import Event
import requests
from Registration import Registration, ClientHost
from FileLeaf import FileLeaf
from collections import defaultdict

all_storageserver_files = set()
storageserver_file_map = defaultdict(set)
replica_report = defaultdict(dict)
registered_storageserver = list()
system_root = dict()
system_root[''] = dict()

registration_api = Flask('registration_api')


@registration_api.route('/register', methods=['POST'])
def register_server():
    """
    :exception IllegalStateException, if this storage client already registered.
    If success, return a list of duplicate files to delete on the local storage of the registering storage server.
    ref: https://stackoverflow.com/questions/20001229/how-to-get-posted-json-in-flask
    """
    request_content = request.json

    requested_storageserver = Registration(request_content["storage_ip"], request_content["client_port"],
                                           request_content["command_port"], request_content["files"])
    # check duplicate, return 409
    if requested_storageserver in registered_storageserver:
        return make_response(jsonify(
            {
                "exception_type": "IllegalStateException",
                "exception_info": "This storage client already registered."
            }), 409)

    # no duplicate, return
    duplicate_files = find_duplicate_files(requested_storageserver)
    add_files(requested_storageserver, duplicate_files)

    registered_storageserver.append(requested_storageserver)
    return make_response(jsonify({
        "files": duplicate_files
    }), 200)


def find_duplicate_files(requested_storageserver):
    duplicate_files = []
    for file in requested_storageserver.files:
        if file in all_storageserver_files:
            duplicate_files.append(file)
    return duplicate_files


def construct_file_tree(files_to_construct):
    for singlefile in files_to_construct:
        file_path = singlefile.split("/")
        dir = system_root
        for directory in file_path[:-1]:
            if directory not in dir:
                dir[directory] = dict()
            dir = dir[directory]


def add_files(requested_storageserver, duplicate_files):
    # construct file dictionary tree
    # example: system_root = {"tmp":{"dist-systems-0"} for /tmp/dist-systems-0
    construct_file_tree(requested_storageserver.files)

    # dive into the file root
    for singlefile in requested_storageserver.files:
        file_path = singlefile.split("/")
        filename = file_path[-1]
        dir = system_root
        for directory in file_path[:-1]:
            dir = dir[directory]

        # now in the file dir
        if filename in dir:
            duplicate_files.append(singlefile)
        else:
            if "files" in dir:
                dir["files"].append(filename)
                dir["fileleaf"].append(FileLeaf(filename))
            else:
                dir["files"] = list([filename])
                dir["fileleaf"] = list([FileLeaf(filename)])

    # get file to add
    # get file in requested_storageserver but not in duplicate files
    add_list = [file for file in requested_storageserver.files if (file not in duplicate_files)]
    # update all_storageserver_files set using union
    all_storageserver_files.update(set(add_list))

    # add to storageserver_file_map
    # add requested_storageserver.files and minus replica
    register_keypair = (requested_storageserver.storage_ip, requested_storageserver.client_port)
    storageserver_file_map[register_keypair].update(
        set(requested_storageserver.files).union(set(add_list)))

    # record initial replica
    for file in add_list:
        if file not in replica_report:
            replica_report[file]["storage_servers"] = list()
            replica_report[file]["access"] = 1
            replica_report[file]["replicated"] = False
        if requested_storageserver.command_port not in replica_report[file]["storage_servers"]:
            replica_report[file]["storage_servers"].append(requested_storageserver.command_port)


'''
ref https://stackoverflow.com/questions/20001229/how-to-get-posted-json-in-flask
'''


def send_deletion_request(path, command_port):
    response = json.loads(
        requests.post("http://localhost:" + str(command_port) + "/storage_delete",
                      json={"path": path}).text
    )
    return response["success"]


def start_registration_api():
    registration_api.run(host='localhost', port=8090)


service_api = Flask('service_api')


@service_api.route('/is_valid_path', methods=['POST'])
def is_valid_path():
    request_content = request.json
    checkpath = path_invalid(request_content['path'])
    if checkpath == "valid":
        return make_response(jsonify({"success": True}), 200)
    else:
        return make_response(jsonify({"success": False}), 404)


@service_api.route('/getstorage', methods=['POST'])
def get_storage():
    request_content = request.json
    stroage_ip, client_port = get_storage_info(request_content["path"])
    requested_path = request_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # FileNotFoundException
    if stroage_ip is None or client_port is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)
    else:
        return make_response(jsonify({
            "server_ip": stroage_ip,
            "server_port": client_port
        }), 200)


def path_cleaner(path):
    path_list = path.strip().split("/")
    cleaner = [path_list[0]]
    cleaner.extend([x for x in path_list[1:] if x])
    return cleaner


'''
Lists the contents of a directory.
'''


@service_api.route('/list', methods=['POST'])
def list_contents():
    request_content = request.json
    requested_path = request_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    file_list = list_helper(requested_path)

    if file_list is None or len(file_list) == 0:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "given path does not refer to a directory."
        }), 400)

    return make_response(jsonify({"files": file_list}), 200)


def list_helper(path):
    path_list = path.split("/")
    if path == '/':
        path_list = path_list[:-1]

    # dive into dir
    dir = system_root
    for directory in path_list:
        if directory not in dir and not (directory == ''):
            return None
        dir = dir[directory]

    list_contents = list()
    if "files" in dir:
        list_contents.extend(dir["files"])

    dir_keys = dir.keys()
    keys_to_remove = ["files", "fileleaf"]
    keys_to_append = [key for key in dir_keys if key not in keys_to_remove]
    list_contents.extend(keys_to_append)
    return list_contents


"""
Determines whether a path refers to a directory.  
"""


@service_api.route('/is_directory', methods=['POST'])
def check_directory():
    requested_content = request.json
    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    if requested_path == "/":
        return make_response(jsonify({
            "success": True}), 200)

    success = is_directory_helper(requested_content["path"])

    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)

    return make_response(jsonify({"success": success}), 200)


@service_api.route('/is_file', methods=['POST'])
def check_directory():
    requested_content = request.json
    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    if requested_path == "/":
        return make_response(jsonify({
            "success": True}), 200)

    success = is_file_helper(requested_content["path"])

    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)

    return make_response(jsonify({"success": success}), 200)


def is_directory(name, root):
    if name in root:
        return True
    if "files" in root and name in root["files"]:
        return False
    return None

def is_file(name, root):
    if name in root:
        return False
    if "files" in root and name in root["files"]:
        return True

def is_file_helper(path):
    path_list = path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if (directory not in root) and not (directory == ''):
            return None
        root = root[directory]

    if "files" in root and dir_name in root["files"]:
        return False
    return is_file(dir_name, root)

def is_directory_helper(path):
    path_list = path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if (directory not in root) and not (directory == ''):
            return None
        root = root[directory]

    return is_directory(dir_name, root)


@service_api.route('/create_directory', methods=['POST'])
def create_directory():
    requested_content = request.json
    dir_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

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
    path_list = path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if directory not in root and not (directory == ''):
            return None
        root = root[directory]

    # dir already exists
    # dirname is a file
    if (dir_name in root) or ("files" in root and dir_name in root["files"]):
        return False

    # create directory
    root[dir_name] = dict()
    return True


@service_api.route('/create_file', methods=['POST'])
def create_file():
    requested_content = request.json
    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    if requested_path == "/":
        return make_response(jsonify({"success": False}), 200)

    success = create_file_helper(requested_content["path"])

    # FileNotFoundException
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "parent directory does not exist."
        }), 400)

    if success:
        all_storageserver_files.add(requested_content["path"])

    return make_response(jsonify({"success": success}), 200)

"""
ref https://stackoverflow.com/questions/16877422/whats-the-best-way-to-parse-a-json-response-from-the-requests-library
"""
def create_file_helper(path):
    path_list = path_cleaner(path)
    file_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if directory not in root and not (directory == ''):
            return None
        if directory not in root and directory == '':
            root[directory] = dict()
        root = root[directory]

    # file already exists
    if (file_name in root) or ("files" in root and file_name in root["files"]):
        return False

    # send request to "http://localhost:command_port/storage_create"
    command_port = str(registered_storageserver[0].command_port)
    response = json.loads(
        requests.post("http://localhost:" + command_port + "/storage_create",
                      json={"path": path}).text
    )

    success = response["success"]

    # if success create file
    if success:
        if "files" not in root:
            root["files"] = [file_name]
            root["fileleaf"] = list([FileLeaf(file_name)])
        else:
            root["files"].append(file_name)
            root["fileleaf"] = list([FileLeaf(file_name)])

    return success


def get_storage_info(path_to_find):
    for storageserver in storageserver_file_map:
        if path_to_find in storageserver_file_map[storageserver]:
            return storageserver
    return (None, None)


def path_invalid(dir_path):
    if len(dir_path) == 0:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }), 400)
    if not (dir_path[0] == '/') or ':' in dir_path:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "path has invalid format"
        }), 400)
    return "valid"

@service_api.route('/delete', methods=['POST'])
def delete_file():


def start_service_api():
    service_api.run(host='localhost', port=8080)


if __name__ == '__main__':
    Thread(target=start_registration_api).start()
    start_service_api()
