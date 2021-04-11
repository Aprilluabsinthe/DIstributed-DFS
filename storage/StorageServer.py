import base64
import os
import shutil
import sys
from threading import Thread

import requests
from flask import Flask, json, request, make_response, jsonify

storage_client_api = Flask('storage_client_api')
storage_command_api = Flask('storage_command_api')

register_port = None
client_port = None
command_port = None
root_dir = None
system_root = {}
system_root[root_dir] = {}
all_files_in_ss = set()


"""
**************************************************** helper functions ****************************************************

**************************************************** helper functions ****************************************************
"""

def generate_full_path(dir, file):
    if file.startswith("/") is False:
        return dir + '/' + file
    return dir + file

def path_invalid(path):
    if not path or len(path) == 0:
        return {
            "exception_type": "IllegalArgumentException",
            "exception_info": "path can not be None"
        }
    if not (path[0] == '/') or ':' in path:
        return {
            "exception_type": "IllegalArgumentException",
            "exception_info": "path has invalid format"
        }
    return "valid"


def is_valid_file(filename):
    checkpath = path_invalid(filename)
    if checkpath != "valid":
        return False
    if filename == "/":
        return False

    try:
        if os.path.isdir(filename):
            return False
        if os.path.isfile(filename):
            return True
    except:
        return False


def is_valid_dir(filename):
    checkpath = path_invalid(filename)
    if checkpath != "valid":
        return False
    if filename == "/":
        return True
    try:
        if os.path.isdir(filename):
            return False
        if os.path.isfile(filename):
            return True
    except:
        return False


def check_offset(offset):
    # If the sequence specified by `offset` and `length` is outside the bounds of the file, or if `length` is negative.
    if offset < 0:
        return {
            "exception_type": "IndexOutOfBoundsException",
            "exception_info": "given offset is negative"
        }
    return "valid"


def check_index(offset, length, file):
    file_size = get_size_helper(file)
    if offset < 0 or length < 0:
        return {
            "exception_type": "IndexOutOfBoundsException",
            "exception_info": "given offset is negative"
        }
    if (offset + length > file_size) and (file_size > 0):
        return {
            "exception_type": "IndexOutOfBoundsException",
            "exception_info": "offset + length out of boudn"
        }
    return "valid"


def to_parent_path(filepath):
    file_path = filepath.split("/")
    file_dir_name = file_path[-1]
    lenname = len(file_dir_name)
    return filepath[:-1 - lenname]


"""
**************************************************** storage_command_api ****************************************************

**************************************************** storage_command_api ****************************************************
"""


@storage_command_api.route('/storage_create', methods=['POST'])
def storage_create_api():
    requested_content = request.json
    requested_path = requested_content["path"]

    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return make_response(jsonify(check_path), 400)

    if requested_path == "/":
        return make_response(jsonify({"success": False}), 200)

    full_path = generate_full_path(root_dir,requested_path)
    success = file_create_helper(full_path)

    # update record
    all_files_in_ss.update(set(update_all_files_record()))

    return make_response(jsonify({"success": success}), 200)


def file_create_helper(filename,is_dir=False):
    """:param:filename:
    https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory
    https://www.geeksforgeeks.org/create-an-empty-file-using-python/
    """
    directory = to_parent_path(filename)
    if os.path.exists(filename):
        return False

    try:
        # Creates a new directory
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Creates a new dir
        if is_dir:
            if not os.path.exists(filename):
                os.makedirs(filename)
        # Creates a new file
        else:
            with open(filename, 'w') as fp: pass
        return True
    except:
        return False

def file_create_dir_helper(filename):
    """:param:filename:
    https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory
    https://www.geeksforgeeks.org/create-an-empty-file-using-python/
    """
    directory = to_parent_path(filename)
    if os.path.exists(filename):
        return False
    if not os.path.exists(directory):
        try:
            # Creates a new directory
            os.makedirs(directory)
            # if not os.path.exists(directory):
            #     os.makedirs(directory)
        except:
            return False
    # Creates a new file
    with open(filename, 'w') as fp:
        pass
    return True

@storage_command_api.route('/storage_delete', methods=['POST'])
def storage_delete_api():
    requested_content = request.json
    requested_path = requested_content["path"]

    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return make_response(jsonify(check_path), 400)

    if requested_path == "/":
        return make_response(jsonify({"success": False}), 200)

    full_path = generate_full_path(root_dir,requested_path)
    delete_success = delete_file_helper(full_path)

    # all_files_in_ss = update_all_files_record()
    all_files_in_ss.update(set(update_all_files_record()))

    return make_response(jsonify({"success": delete_success}), 200)


def delete_file_helper(path):
    """
    ref: https://stackoverflow.com/questions/6996603/how-to-delete-a-file-or-folder
    :param : path to delete
    :return: True if success , else False
    """

    if not os.path.exists(path):
        return False

    if os.path.isdir(path):
        shutil.rmtree(path)
        return True

    if os.path.isfile(path):
        os.remove(path)
        return True

    return True


@storage_command_api.route('/storage_copy', methods=['POST'])
def storage_copy_api():
    requested_content = request.json
    requested_path = requested_content["path"]
    server_port = requested_content["server_port"]

    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return check_path, 400

    requested_size = request_for_size(path=requested_path, storage_port=server_port)
    read_result = request_for_read(path=requested_path, offset=0, length=requested_size, storage_port=server_port)

    dst_path = generate_full_path(root_dir,requested_path)
    success_create = file_create_helper(dst_path)

    if success_create:
        try:
            success_write = file_write_helper(dst_path, 0, read_result)
        except:
            return make_response(jsonify({
                "exception_type": "IOException",
                "exception_info": "the file write cannot be completed on the server."
            }), 200)

    # update record
    # all_files_in_ss = update_all_files_record()
    all_files_in_ss.update(set(update_all_files_record()))

    success = success_create and success_write
    return make_response(jsonify({"success": success}), 200)


"""
**************************************************** Storage api ****************************************************

**************************************************** Storage api ****************************************************
"""


@storage_client_api.route('/storage_size', methods=['POST'])
def storage_size_api():
    requested_content = request.json
    requested_path = requested_content["path"]

    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return make_response(jsonify(check_path), 400)

    full_path = generate_full_path(root_dir,requested_path)

    if is_valid_file(full_path):
        size = get_size_helper(full_path)
        return make_response(jsonify({"size": size}), 200)
    else:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 404)
    return make_response(jsonify(response_obj), response_code)


def get_size_helper(file_name):
    file_size = os.path.getsize(file_name)
    return file_size


def request_for_size(path, storage_port):
    requested_content = {"path": path}
    response = json.loads(
        requests.post("http://localhost:" + str(storage_port) + "/storage_size",
                      json=requested_content).text
    )
    return response["size"]


@storage_client_api.route('/storage_read', methods=['POST'])
def storage_read_api():
    requested_content = request.json
    requested_path = requested_content["path"]
    requested_offset = requested_content["offset"]
    requested_length = requested_content["length"]

    # IllegalArgumentException
    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return make_response(jsonify(check_path), 400)

    # IndexOutOfBoundsException
    check_offset_content = check_offset(requested_offset)
    if check_offset_content != "valid":
        return make_response(jsonify(check_offset_content), 400)

    full_path = generate_full_path(root_dir,requested_path)
    # full_path = root_dir + requested_path

    if is_valid_file(full_path):
        # IndexOutOfBoundsException
        check_index_validation = check_index(requested_offset, requested_length, full_path)
        if check_index_validation != "valid":
            return make_response(jsonify(check_index_validation), 400)
        else:
            try:
                read_bytes_encoded = file_read_helper(full_path, requested_offset, requested_length)
            except:
                # IOException
                return make_response(jsonify({
                    "exception_type": "IOException",
                    "exception_info": "the file write cannot be completed on the server."
                }), 200)
            return make_response(jsonify({"data": read_bytes_encoded}), 200)
    else:
        # FileNotFoundException
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 404)


def request_for_read(path, offset, length, storage_port):
    requested_content = {
        "path": path,
        "offset": offset,
        "length": length
    }
    response = json.loads(
        requests.post("http://localhost:" + str(storage_port) + "/storage_read",
                      json=requested_content)
    )
    return response['data']


def file_read_helper(file_name, offset, length):
    """
    file_read_helper
    :param file_name the name(whole path) to be read from
    :param offset the offset of reading files
    :param length the length of reading
    """
    try:
        file_obj = open(file_name, "r")
        # ref: https://stackoverflow.com/questions/3299213/python-how-can-i-open-a-file-and-specify-the-offset-in-bytes
        file_obj.seek(offset)
        read_bytes = file_obj.read(length)

        # Encode the string using the codec registered for encoding
        # ref; https://stackabuse.com/encoding-and-decoding-base64-strings-in-python/
        read_bytes = read_bytes.encode('ascii')
        read_bytes = base64.b64encode(read_bytes)
        read_bytes = read_bytes.decode('ascii')

        file_obj.close()
        return read_bytes
    except:
        # any operation error
        raise IOError


@storage_client_api.route('/storage_write', methods=['POST'])
def storage_write_api():
    # read from request
    requested_content = request.json
    requested_path = requested_content["path"]
    requested_offset = requested_content["offset"]
    requested_data = requested_content["data"]

    # IllegalArgumentException
    check_path = path_invalid(requested_path)
    if check_path != "valid":
        return make_response(jsonify(check_path), 400)

    # IndexOutOfBoundsException
    check_offset_content = check_offset(requested_offset)
    if check_offset_content != "valid":
        return make_response(jsonify(check_offset_content), 400)

    full_path = generate_full_path(root_dir,requested_path)
    # full_path = root_dir + requested_path
    if is_valid_file(full_path):
        try:
            write_success = file_write_helper(full_path, requested_offset, requested_data)
        except:
            return make_response(jsonify({
                "exception_type": "IOException",
                "exception_info": "the file write cannot be completed on the server."
            }), 200)
        return make_response(jsonify({"success": write_success}), 200)
    else:
        # FileNotFoundException
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 404)


def file_write_helper(file_name: str, offset: int, data: str):
    try:
        file_obj = open(file_name, "w")
        file_obj.seek(offset)

        data_bytes = data.encode("ascii")
        data_to_write = base64.b64decode(data_bytes)
        data_to_write = data_to_write.decode("ascii")

        file_obj.write(data_to_write)
        file_obj.close()
        return True
    except:
        raise IOError


"""
**************************************************** Register ****************************************************

**************************************************** Register ****************************************************
"""


def walk_through_dir(dirName, dir_obj):
    dir_in_level = os.listdir(dirName)
    all_file_list = list()
    for filename in dir_in_level:
        file_path_with_root = generate_full_path(dirName, filename)
        is_dir = is_valid_dir(file_path_with_root)
        is_file = is_valid_file(file_path_with_root)
        if os.path.isdir(file_path_with_root):
            dir_tree = dict()
            dir_tree = walk_through_dir(file_path_with_root, dir_tree)
            dir_obj[filename] = dir_tree
            if len(dir_tree["files"]) == 0:
                os.rmdir(file_path_with_root)
            all_file_list.extend(dir_tree["files"])
        else:
            all_file_list.append(file_path_with_root[len(root_dir):])
    dir_obj["files"] = [] + all_file_list
    return dir_obj


def update_all_files_record():
    system_root[root_dir] = walk_through_dir(root_dir, system_root[root_dir])
    all_files_in_ss = system_root[root_dir]["files"]
    return all_files_in_ss


def register():
    all_files_in_ss = update_all_files_record()

    request_content = {
        "storage_ip": "127.0.0.1",
        "client_port": client_port,
        "command_port": command_port,
        "files": all_files_in_ss
    }
    response = requests.post('http://localhost:' + register_port + "/register", json=request_content)

    # already registered
    if response.status_code == 409:
        return False

    response_content = json.loads(response.text)
    duplicate_files = response_content["files"]
    for file in duplicate_files:
        full_path = generate_full_path(root_dir, file)
        delete_file_helper(full_path)

    all_files_in_ss = update_all_files_record()
    return True


def start_client(client_port):
    storage_client_api.run(host='localhost', port=int(client_port))


def start_command(command_port):
    storage_command_api.run(host='localhost', port=int(command_port))


if __name__ == '__main__':
    # read arguments
    client_port = sys.argv[1]
    command_port = sys.argv[2]
    register_port = sys.argv[3]
    root_dir = sys.argv[4]
    system_root[root_dir] = {}

    # create root dir, example: /tmp/dist-systems-1
    file_create_helper(root_dir,True)

    # apply for register
    register()


    Thread(target=start_command, args=(command_port,)).start()
    start_client(client_port)
