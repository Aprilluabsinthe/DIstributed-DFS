from flask import Flask, request, json, make_response, jsonify
from threading import Thread, Condition, RLock
from threading import Event
import requests
from Registration import Registration, ClientHost, LockRequestQueue
from FileLeaf import FileLeaf, DirLockReport
from collections import defaultdict

from FileLock import FileLock


"""
member variables
"""
# system_root dictionary
system_root = dict()
root_report = dict()
system_root[''] = dict()

# for register and globla storage
all_storageserver_files = set()
storageserver_file_map = defaultdict(set)
file_server_map = defaultdict(tuple)
registered_storageserver = list()

# for replica
replica_report = defaultdict(dict)

# for queueing and locking
lock_queue_report = LockRequestQueue()
exclusive_wait_queue = list()


"""
****************************************** start of functions ******************************************

****************************************** start of functions ******************************************
"""




"""
******************************************** registration API ********************************************

******************************************** registration API ********************************************
"""
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

    # no duplicate, return duplicate files
    duplicate_files = find_duplicate_files(requested_storageserver)
    duplicate_files = add_files_and_storageservers(requested_storageserver, duplicate_files)
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


def to_parent_dir(filepath):
    file_path = filepath.split("/")
    dir = system_root
    for directory in file_path[:-1]:
        dir = dir[directory]
    return dir


def to_parent_path(filepath):
    file_path = filepath.split("/")
    file_dir_name = file_path[-1]
    lenname = len(file_dir_name)
    return filepath[:-1 - lenname]


def add_files_and_storageservers(requested_storageserver, duplicate_files):
    # construct file dictionary tree
    # example: system_root = {"tmp":{"dist-systems-0"} for /tmp/dist-systems-0
    construct_file_tree(requested_storageserver.files)
    # add new files to it's directory
    duplicate_files = add_file_to_directory(requested_storageserver, duplicate_files)
    # add requested_storageserver to map
    duplicate_files = add_storageserver_to_map(requested_storageserver, duplicate_files)
    # add requested_storageserver to registered_storageserver set
    registered_storageserver.append(requested_storageserver)
    return duplicate_files


def add_file_to_directory(requested_storageserver, duplicate_files):
    # dive into the parent root
    for singlefile in requested_storageserver.files:
        file_path = singlefile.split("/")
        filename = file_path[-1]
        dir = to_parent_dir(singlefile)

        # now in the file dir
        if filename in dir:
            duplicate_files.append(singlefile)
        else:
            if "files" in dir:
                dir["files"].append(filename)
                dir["fileleaf"].append(FileLeaf(filename))
                dir["filelock"].append(FileLock(filename))
            else:
                dir["files"] = list([filename])
                dir["fileleaf"] = list([FileLeaf(filename)])
                dir["filelock"] = list([FileLock(filename)])
    return duplicate_files


def add_storageserver_to_map(requested_storageserver, duplicate_files):
    add_list = [file for file in requested_storageserver.files if (file not in duplicate_files)]
    # update all_storageserver_files set using union
    all_storageserver_files.update(set(add_list))

    # add to storageserver_file_map
    # add requested_storageserver.files and minus replica
    register_keypair = (requested_storageserver.storage_ip, requested_storageserver.client_port)
    storageserver_file_map[register_keypair].update(set(requested_storageserver.files).union(set(add_list)))
    for file in set(requested_storageserver.files).union(set(add_list)):
        if file not in file_server_map:
            file_server_map[file] = (requested_storageserver.storage_ip, requested_storageserver.client_port)

    return duplicate_files



"""
******************************************** service API ********************************************

******************************************** service API ********************************************
"""

service_api = Flask('service_api')

"""
The path string should be a sequence of components delimited with forward slashes. 
Empty components are dropped.
The string must begin with a forward slash.
 And the string must not contain any colon character.
"""


@service_api.route('/is_valid_path', methods=['POST'])
def is_valid_path():
    request_content = request.json
    checkpath = path_invalid(request_content['path'])
    if checkpath == "valid":
        return make_response(jsonify({"success": True}), 200)
    else:
        return make_response(jsonify({"success": False}), 404)


"""
> If the client intends to perform calls only to `read` or `size` after obtaining the storage server stub,
> it should lock the file for shared access before making this call.
> If it intends to perform calls to `write`, it should lock the file for exclusive access.  
"""


@service_api.route('/getstorage', methods=['POST'])
def get_storage():
    request_content = request.json
    stroage_ip, server_port = get_storage_map(request_content["path"])
    # stroage_ip,server_port = get_filestorage_map(request_content["path"])
    requested_path = request_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # FileNotFoundException
    if stroage_ip is None or server_port is None:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "File/path cannot be found."
        }), 400)
    else:
        return make_response(jsonify({
            "server_ip": stroage_ip,
            "server_port": server_port
        }), 200)


def path_cleaner(path):
    path_list = path.strip().split("/")
    cleaner = [path_list[0]]
    cleaner.extend([x for x in path_list[1:] if x])
    return cleaner


'''
Lists the contents of a directory.
> The directory should be locked for shared access before this operation is performed,
> because this operation reads the directory's child list.  
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

    keys_to_append = [key for key in dir.keys() if key not in ["files", "fileleaf", "filelock"]]

    list_contents.extend(keys_to_append)
    return list_contents


"""
Determines whether a path refers to a directory.  
> The parent directory should be locked for shared access before this operation is performed.
> This is to prevent the object in question from being deleted or re-created while this call is in progress.
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
def check_file():
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
    if "files" in root and name in root["files"]:
        return True


def is_file_helper(path):
    path_list = path_cleaner(path)
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
    path_list = path_cleaner(path)
    dir_name = path_list[-1]

    root = system_root
    for directory in path_list[:-1]:
        if (directory not in root) and not (directory == ''):
            return None
        root = root[directory]

    return is_directory(dir_name, root)


"""
> The parent directory should be locked for exclusive access before this operation is performed.
"""


@service_api.route('/create_directory', methods=['POST'])
def create_directory():
    requested_content = request.json
    dir_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    # if dir_path == "/":
    #     return make_response(jsonify({"success": False}), 200)

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


"""
The parent directory should be locked for exclusive access before this operation is performed.
"""


@service_api.route('/create_file', methods=['POST'])
def create_file():
    requested_content = request.json
    requested_path = requested_content["path"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    # if requested_path == "/":
    #     return make_response(jsonify({"success": False}), 200)

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
            root["RWlock"] = list([FileLock(file_name)])
        else:
            root["files"].append(file_name)
            root["fileleaf"].append(FileLeaf(file_name))
            root["RWlock"].append(FileLock(file_name))

    return success


def get_storage_map(path_to_find):
    for storageserver in storageserver_file_map:
        if path_to_find in storageserver_file_map[storageserver]:
            return storageserver
    return None, None


def get_filestorage_map(path_to_find):
    if path_to_find in path_to_find:
        return file_server_map[path_to_find]
    return None, None


def path_invalid(dir_path):
    if not dir_path or len(dir_path) == 0:
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


"""
********************************************* checkpoint *********************************************

********************************************* checkpoint *********************************************
"""

"""
ref: https://stackoverflow.com/questions/53780267/an-equivalent-to-java-volatile-in-python
"""


@service_api.route('/lock', methods=['POST'])
def lock_path():
    requested_content = request.json
    requested_path = requested_content["path"]
    exclusive = requested_content["exclusive"]

    # IllegalArgumentException
    checkpath = path_invalid(requested_path)
    if checkpath != "valid":
        return checkpath

    is_dir = is_directory_helper(requested_path)
    is_file = is_file_helper(requested_path)

    if (is_file or (not is_dir)) and (requested_path not in all_storageserver_files):
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "path cannot be found."
        }), 400)

    if requested_path == "/":
        return lock_root_operation(exclusive)
    else:
        if is_dir:
            return lock_directory_operation(requested_path, exclusive)
        else:
            return lock_file_operation(requested_path, exclusive)


def lock_root_operation(exclusive=True):
    requested_path = "/"
    filelock, can_lock = acquire_lock_and_ability(is_root=True, exclusive_lock=exclusive)

    if not filelock:  # never be locked
        if not exclusive and can_lock:
            lock_queue_report.shared_counter += 1
    else:  # has locking report
        can_direct_lock = (lock_queue_report.queue_size == 0 and can_lock)

        # no need for queue
        if can_direct_lock:
            add_to_shared_queue(exclusive)

        # queueing design
        if not can_direct_lock:  # lock_queue_report.queue_size > 0 or not can_lock
            if exclusive and lock_queue_report.shared_counter > 0:  # can not operate exclusive lock, should queue
                lock_request = (filelock, requested_path, exclusive)
                exclusive_wait_queue.append(lock_request)
            else:  # can operate exclusive lock
                if len(
                        exclusive_wait_queue) == lock_queue_report.queue_size:  # if all the queueing request are all exclusive
                    filelock = Event()
                elif lock_queue_report.queue:  # use last lock
                    filelock = lock_queue_report.queue[-1][0]
                lock_request = (filelock, requested_path, exclusive)

            # append to global queue and wait
            lock_queue_report.queue.append(lock_request)
            lock_queue_report.queue_size += 1
            lock_request[0].wait()
            # operate the first request
            exclusive = lock_queue_report.queue.pop(0)[2]
            lock_queue_report.queue_size -= 1
            add_to_shared_queue(exclusive)

    success = do_lock(is_root=True, exclusive_lock=exclusive)
    content = "" if success else "Lock Failed"
    return make_response(content, 200)


def add_to_shared_queue(exclusive):
    if not exclusive and lock_queue_report.queue_size == 0:
        lock_queue_report.shared_counter += 1


def lock_directory_operation(path, exclusive):
    success_add = add_and_wait(path=path, exclusive_lock=exclusive)
    if not success_add:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "directory cannot be found."
        }), 400)

    success_lock = do_lock(path=path, exclusive_lock=exclusive)
    content = "" if success_lock else "Lock Failed"
    return make_response(content, 200)


def lock_file_operation(path, exclusive):
    success_add = add_and_wait(path=path, exclusive_lock=exclusive)
    if not success_add:
        return make_response(jsonify({
            "exception_type": "FileNotFoundException",
            "exception_info": "directory cannot be found."
        }), 400)

    if lock_queue_report.queue_size > 0:
        queued_lock_request = lock_queue_report.queue.pop(0)
        lock_queue_report.queue_size -= 1
        path, exclusive = queued_lock_request[1], queued_lock_request[2]

    success = do_lock(path=path, exclusive_lock=exclusive)
    content = "" if success else "Lock Failed"
    return make_response(content, 200)


def add_and_wait(path, exclusive_lock):
    filelock, can_lock = acquire_lock_and_ability(path=path, exclusive_lock=exclusive_lock)

    if filelock is None and not can_lock:
        return False

    can_direct_lock = (lock_queue_report.queue_size == 0 and can_lock)

    if not can_direct_lock:
        if filelock:
            lock_request = (filelock, path, exclusive_lock)
            lock_queue_report.queue.append(lock_request)
            lock_queue_report.queue_size += 1
            lock_request[0].wait()
        else:
            queued_lock_request = lock_queue_report.queue[0]
            qfilelock, qcan_lock = acquire_lock_and_ability(path=queued_lock_request[1],
                                                            exclusive_lock=queued_lock_request[2])
            if not qcan_lock:
                return True
    return True


def check_upper_dir_locker(path):
    path_list = path.split("/")
    parent_dir = system_root
    for dir_name in path_list[:-1]:
        current_dir = parent_dir[dir_name]
        if ("dirlock" in current_dir) and current_dir["dirlock"].exclusive:
            return current_dir["dirlock"].filelock, False

        parent_dir = parent_dir[dir_name]
    return None, True


def dir_in_parent_directory(dir_name, parent_dir):
    if dir_name in parent_dir:
        return True
    return False


def dir_lock_and_ability_helper(is_root=False, directory=None, dir_name=None, exclusive_lock=True):
    if not is_root and not dir_in_parent_directory(dir_name, directory):
        return None, False

    lock_report = system_root[''] if is_root else directory[dir_name]
    # no locking record, can be locked
    if not lock_report or "dirlock" not in lock_report:
        return None, True

    # has locking record, lock is exclusive
    if lock_report["dirlock"].locked and (lock_report["dirlock"].exclusive or exclusive_lock):
        return lock_report["dirlock"].filelock, False

    # has locking record, lock is shared
    return (lock_report["dirlock"].filelock if is_root else None), True


def file_in_parent_directory(file_name, parent_dir):
    if "fileleaf" not in parent_dir:
        return False
    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            return True
    return False


def file_lock_and_ability_helper(parent_dir=None, file_name=None, exclusive_lock=True):
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


"""
whether the path can acquire a exclusive/share lock
"""


def acquire_lock_and_ability(is_root=False, path=None, exclusive_lock=True):
    if is_root:
        return dir_lock_and_ability_helper(is_root=True, exclusive_lock=exclusive_lock)
    else:
        # check if upper directories can be locked
        lock, ablility = check_upper_dir_locker(path)
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
    filelock, can_lock = acquire_lock_and_ability(is_root, path, exclusive_lock)
    if not can_lock:
        return False

    if path == "/":
        is_root = True

    if is_root:
        root_report = system_root['']

        if "dirlock" not in root_report:
            root_report["dirlock"] = DirLockReport()
            root_report["dirlock"].acquire(exclusive=exclusive_lock)
        elif (not root_report["dirlock"].locked) or (root_report["dirlock"].locked and not root_report["dirlock"].exclusive):
            root_report["dirlock"].acquire(exclusive=exclusive_lock)

        return True
    else:
        is_dir = is_directory_helper(path)
        path_list = path.split("/")
        dir_or_file_name = path_list[-1]

        # lock dirs from up to bottom
        lock_upper_dir(path)

        # lock according to directory or file
        parent_dir = to_parent_dir(path)
        if is_dir and dir_in_parent_directory(dir_or_file_name, parent_dir):
            return lock_dirctory(dir_or_file_name, parent_dir, exclusive_lock)
        elif file_in_parent_directory(dir_or_file_name, parent_dir):
            return lock_file(dir_or_file_name, parent_dir, exclusive_lock)


def lock_upper_dir(path):
    path_list = path.split("/")
    parent_dir = system_root

    for dir_name in path_list[:-1]:
        child_dir = parent_dir[dir_name]

        if "dirlock" not in child_dir:
            child_dir["dirlock"] = DirLockReport()
        child_dir["dirlock"].acquire(exclusive=False)

        parent_dir = parent_dir[dir_name]


def unlock_upper_dir(path):
    path_list = path.split("/")
    parent_dir = system_root

    for dir_name in path_list[:-1]:
        child_dir = parent_dir[dir_name]

        if "dirlock" not in child_dir:
            child_dir["dirlock"] = DirLockReport()

        # TODO: check the locked flag
        child_dir["dirlock"].release()

        parent_dir = parent_dir[dir_name]


def lock_dirctory(dir_name, parent_dir, exclusive_lock):
    target_dir = parent_dir[dir_name]

    if "dirlock" not in target_dir:
        target_dir["dirlock"] = DirLockReport()
    if not target_dir["dirlock"].locked:
        target_dir["dirlock"].acquire(exclusive=exclusive_lock)
    return True


def lock_file(file_name, parent_dir, exclusive_lock):
    if "fileleaf" not in parent_dir:
        return False

    for file in parent_dir["fileleaf"]:
        if file.file_name == file_name:
            if not file.locked:
                file.acquire(exclusive_lock)
            return True
    return False


def do_unlock(is_root=False, path=None):
    if is_root:
        root_report = system_root['']
        if "dirlock" not in root_report:
            return True
        if not root_report["dirlock"].exclusive:
            lock_queue_report.shared_counter -= 1
        if lock_queue_report.shared_counter == 0:
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
        is_dir = is_directory_helper(path)
        path_list = path.split("/")
        dir_or_file_name = path_list[-1]
        unlock_upper_dir(path)
        parent_dir = to_parent_dir(path)

        if is_dir and dir_in_parent_directory(dir_or_file_name, parent_dir):
            return unlock_dirctory(dir_or_file_name, parent_dir)
        else:
            return unlock_file(dir_or_file_name, parent_dir)


def unlock_upper_dir(path):
    path_list = path.split("/")
    parent_dir = system_root
    for dir_name in path_list[:-1]:
        parent_dir = parent_dir[dir_name]
        if "dirlock" in parent_dir and parent_dir["dirlock"].locked:
            parent_dir["dirlock"].release()

def unlock_dirctory(dir_name, parent_dir):
    if not dir_in_parent_directory(dir_name, parent_dir):
        return False

    target_dir = parent_dir[dir_name]
    if "dirlock" in target_dir and target_dir["dirlock"].locked:
        target_dir["dirlock"].release()
    return True


def unlock_file(file_name, parent_dir):
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
    request_obj = request.json
    dir_path = request_obj["path"]

    # IllegalArgumentException
    checkpath = path_invalid(dir_path)
    if checkpath != "valid":
        return checkpath

    is_directory = is_directory_helper(dir_path)

    if (dir_path not in all_storageserver_files) and (not is_directory):
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


"""
> The parent directory should be locked for exclusive access before this operation is performed.  
"""


@service_api.route('/delete', methods=['POST'])
def delete_file():
    pass


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


def start_service_api():
    service_api.run(host='localhost', port=8080)


if __name__ == '__main__':
    Thread(target=start_registration_api).start()
    service_api.run(host='localhost', port=8080)
