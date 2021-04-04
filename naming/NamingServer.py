from flask import Flask, request, json, make_response, jsonify
from threading import Thread
from threading import Event
import requests
from Registration import Registration, ClientHost
from FileNode import FileNode
from collections import defaultdict

all_storageserver_files = set()
storageserver_file_map = defaultdict(set)
replica_report = defaultdict(dict)
registered_storageserver = list()
system_root = dict()
system_root[''] = dict()
queue = list()
el_queue = list()
shared_lock_counter = 0

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
    has_duplicate = check_duplicate_storageserver(requested_storageserver)
    if has_duplicate:
        return make_response(jsonify(
            {
                "exception_type": "IllegalStateException",
                "exception_info": "This storage client already registered."
            }), 409)

    # no duplicate, return
    duplicate_files = find_duplicate_files(requested_storageserver)
    add_files(requested_storageserver, duplicate_files)

    files_to_delete_dict = dict()
    files_to_delete_dict["files"] = duplicate_files

    registered_storageserver.append(requested_storageserver)
    files_response = files_to_delete_dict

    if "exception_type" in files_response:
        return make_response(jsonify(
            {"exception_type": "IllegalStateException",
             "exception_info": "This storage client already registered."}
        ), 409)

    return make_response(jsonify(files_response), 200, )


def check_duplicate_storageserver(requested_storageserver):
    for storageserver in registered_storageserver:
        if storageserver == requested_storageserver:
            return True
    return False


def find_duplicate_files(requested_storageserver):
    duplicate_files = []
    for file in requested_storageserver.files:
        if file in all_storageserver_files:
            duplicate_files.append(file)
    return duplicate_files


def construct_file_tree(files_to_construct):
    for singlefile in files_to_construct:
        file_path = singlefile.split("/")
        root = system_root
        for directory in file_path[:-1]:
            if directory not in root:
                root[directory] = dict()
            root = root[directory]


def add_files(requested_storageserver, duplicate_files):
    construct_file_tree(requested_storageserver.files)

    for singlefile in requested_storageserver.files:
        file_path = singlefile.split("/")
        filename = file_path[-1]
        root = system_root
        for directory in file_path[:-1]:
            root = root[directory]

        # now in the file dir
        if filename in root:
            duplicate_files.append(singlefile)
        else:
            if "files" in root:
                root["files"].append(filename)
                root["file_nodes"].append(FileNode(filename))
            else:
                root["files"] = list([filename])
                root["file_nodes"] = list([FileNode(filename)])

    # get file to add
    add_list = [file for file in requested_storageserver.files if (file not in duplicate_files)]
    all_storageserver_files.update(set(add_list))

    clienthost_keypair = (requested_storageserver.storage_ip, requested_storageserver.client_port)
    storageserver_file_map[clienthost_keypair].update(
        set(requested_storageserver.files).union(set(add_list)))

    for file in add_list:
        if file not in replica_report:
            replica_report[file]["storage_servers"] = list()
            replica_report[file]["access"] = 1
            replica_report[file]["replicated"] = False
        if requested_storageserver.command_port not in replica_report[file]["storage_servers"]:
            replica_report[file]["storage_servers"].append(requested_storageserver.command_port)


def initiate_deletion(path, command_port):
    req_obj = {
        "path": path,
    }
    resp = requests.post("http://localhost:" + str(command_port) + "/storage_delete",
                         json=req_obj)
    delete_successful = json.loads(resp.text)["success"]
    return delete_successful


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
    ip_addr, port_num = get_storage_info(request_content["path"])
    # clienthost = get_storage_info(request_obj["path"])
    # ip_addr= clienthost.get_storage_ip()
    # port_num = clienthost.get_client_port()
    dir_path = request_content["path"]

    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    if ip_addr is None or port_num is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File not found"
        }), 404)
    else:
        return make_response(jsonify({
            "server_ip": ip_addr,
            "server_port": port_num
        }), 200)


@service_api.route('/list', methods=['POST'])
def list_contents():
    request_content = request.json
    dir_path = request_content["path"]

    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    # file_list = file_system_operations(request_content["path"], list_files=True)
    file_list = list_helper(request_content["path"])
    if file_list is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "given path does not refer to a directory."
        }), 404)

    return make_response(jsonify({
        "files": file_list}), 200)


def list_helper(path):
    path_list = path.split("/")
    if path == '/':
        path_list = path_list[:-1]

    root = system_root
    for directory in path_list:
        if directory not in root and not (directory == ''):
            return None
        root = root[directory]

    contents = list()
    if "files" in root:
        contents.extend(root["files"])
    directories = list(root.keys())
    if "files" in directories:
        del (directories[directories.index("files")])
    if "file_nodes" in directories:
        del (directories[directories.index("file_nodes")])
    contents.extend(directories)
    return contents


@service_api.route('/is_directory', methods=['POST'])
def check_directory():
    request_obj = request.json
    dir_path = request_obj["path"]
    if not dir_path or len(dir_path) == 0:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "File/path cannot be found."
        }), 404)

    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    if dir_path == "/":
        return make_response(jsonify({
            "success": True}), 200)

    success = is_directory_helper(request_obj["path"])
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 404)

    return make_response(jsonify({
        "success": success}), 200)


def is_directory_helper(path):
    path_list = path.split("/")
    clean_list = list()
    clean_list.append(path_list[0])
    for i in range(1, len(path_list)):
        if not (path_list[i] == ''):
            clean_list.append(path_list[i])
    dir_elem = clean_list[-1]

    root = system_root
    for directory in clean_list[:-1]:
        if directory not in root and not (directory == ''):
            return None
        root = root[directory]

    if dir_elem in root:
        return True
    if "files" in root and dir_elem in root["files"]:
        return False


@service_api.route('/create_directory', methods=['POST'])
def create_directory():
    request_obj = request.json
    dir_path = request_obj["path"]
    if len(dir_path) == 0:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "Provided empty path"
        }), 404)

    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    if dir_path == "/":
        return make_response(jsonify({
            "success": False}), 200)

    success = create_directory_helper(request_obj["path"])
    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "parent directory does not exist."
        }), 404)

    return make_response(jsonify({
        "success": success}), 200)


def create_directory_helper(path):
    path_list = path.split("/")
    clean_list = list()
    clean_list.append(path_list[0])
    for i in range(1, len(path_list)):
        if not (path_list[i] == ''):
            clean_list.append(path_list[i])
    dir_elem = clean_list[-1]

    root = system_root
    for directory in clean_list[:-1]:
        if directory not in root and not (directory == ''):
            return None
        root = root[directory]

    if dir_elem in root:
        return False
    if "files" in root and dir_elem in root["files"]:
        return False
    root[dir_elem] = dict()
    return True


@service_api.route('/create_file', methods=['POST'])
def create_file():
    request_obj = request.json
    dir_path = request_obj["path"]

    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    if dir_path == "/":
        return make_response(jsonify({"success": False}), 200)
    success = create_file_helper(request_obj["path"])

    if success is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "parent directory does not exist."
        }), 404)

    if success:
        all_storageserver_files.add(request_obj["path"])

    return make_response(jsonify({"success": success}), 200)


def create_file_helper(path):
    path_list = path.split("/")
    clean_list = list()
    clean_list.append(path_list[0])
    for i in range(1, len(path_list)):
        if not (path_list[i] == ''):
            clean_list.append(path_list[i])
    path_elem = clean_list[-1]
    file_path = {"path": path}

    root = system_root
    for directory in clean_list[:-1]:
        if directory not in root and not (directory == ''):
            return None
        if directory not in root and directory == '':
            root[directory] = dict()
        root = root[directory]

    if path_elem in root:
        return False
    if "files" in root and path_elem in root["files"]:
        return False

    resp = requests.post("http://localhost:" + str(registered_storageserver[0].command_port) + "/storage_create",
                         json=file_path)
    response_json = json.loads(resp.text)

    if response_json["success"]:
        if "files" not in root:
            root["files"] = [path_elem]
            file_node = FileNode(path_elem)
            root["file_nodes"] = [file_node]
        else:
            root["files"].append(path_elem)
            file_node = FileNode(path_elem)
            root["file_nodes"] = [file_node]
        return True
    else:
        return False


def get_storage_info(path_to_find):
    for storageserver in storageserver_file_map:
        if path_to_find in storageserver_file_map[storageserver]:
            return storageserver
    return None, None


def path_invalid(dir_path):
    if len(dir_path) == 0:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "Provided empty path"
        }), 404)
    if not (dir_path[0] == '/') or ':' in dir_path:
        return make_response(jsonify({
            "exception_type": "IllegalArgumentException",
            "exception_info": "Invalid path"
        }), 404)
    return "valid"


def start_service_api():
    service_api.run(host='localhost', port=8080)


if __name__ == '__main__':
    Thread(target=start_registration_api).start()
    start_service_api()
