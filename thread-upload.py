#!/usr/bin/env pytnon
"""thread-upload.py
Box Thread upload tool

Usage:
    thread-upload.py <JWT-FILE> <FILE> [--user=<BOX-USER-ID>] [--folder=<FOLDER-ID>] [--file=<FILE-ID>] [--threads=<THREADS-NUM>]
    thread-upload.py (-h | --help)

Options:
    -h --help                   Show this screen.
    --version                   Show version.
    --user=<BOX-USER-ID>        Box user id. If there is no specification, the service account ID will be used.
    --folder=<FOLDER-ID>        Box folder id. [default: 0]
    --file=<FILE-ID>            Box file id. Updates a file with an already existing file ID; cannot be used in conjunction with FOLDER-ID.
    --threads=<THREADS-NUM>     Number of threads [default: 4].
"""
import os
import sys
import threading

from boxsdk import Client, JWTAuth
from boxsdk.config import API
from docopt import docopt


def check_opt(opt):
    jwt_file = opt['<JWT-FILE>']
    # Check if JWT file exists and is readable
    if not os.path.isfile(jwt_file) or not os.access(jwt_file, os.R_OK):
        print(f"Error: JWT file '{jwt_file}' does not exist or is not readable.")
        sys.exit(1)

    file_path = opt['<FILE>']
    # Check if file exists and is readable
    if not os.path.isfile(file_path) or not os.access(file_path, os.R_OK):
        print(f"Error: File '{file_path}' does not exist or is not readable.")
        sys.exit(1)

    _folder_id = None if opt['--folder'] == '0' else opt['--folder']

    if _folder_id and opt['--file']:
        print(f"Error: Cannot specify both FILE-ID and FOLDER-ID.")
        sys.exit(1)

    if opt['--file']:
        try:
            int(opt['--file'])
        except Exception as e:
            print(f"Error: File id '{opt['--file']}' is not a number.")
            sys.exit(1)

    if opt["--folder"]:
        try:
            int(opt['--folder'])
        except Exception as e:
            print(f"Error: Folder id '{opt['--folder']}' is not a number.")
            sys.exit(1)

    user_id = opt['--user']
    if user_id:
        try:
            int(user_id)
        except Exception as e:
            print(f"Error: User id '{user_id}' is not a number.")
            sys.exit(1)

    threads_num = opt['--threads']
    try:
        int(threads_num)
    except Exception as e:
        print(f"Error: Threads number '{threads_num}' is not a number.")
        sys.exit(1)


def create_client(jwt_file, user_id=None):
    user = None
    try:
        if user_id:
            sa_client = Client(create_auth(jwt_file))
            user = sa_client.user(user_id=user_id).get()
    except Exception as e:
        print(f"Error: User id '{user_id}' not found in box.")
        sys.exit(1)

    return Client(create_auth(jwt_file, user=user))


def create_auth(jwt_file, user=None):
    return JWTAuth.from_settings_file(jwt_file, user=user)


def get_folder(client, folder_id):
    try:
        folder = client.folder(folder_id).get()
        return folder
    except Exception as e:
        print(f"Error: Folder id '{folder_id}' not found in box.")
        sys.exit(1)


def main(opt):
    jwt_file = opt['<JWT-FILE>']
    file_path = opt['<FILE>']
    user_id = opt['--user']
    file_id = opt['--file']
    folder_id = opt['--folder']
    threads = opt['--threads']

    # Set Upload Threads numbers
    API.CHUNK_UPLOAD_THREADS = int(threads)

    # Get Client
    client = create_client(jwt_file, user_id=user_id)

    # Get Folder if specify folder id
    if folder_id:
        folder = get_folder(client, folder_id)

    if file_id:
        file = client.file(file_id).get()

    import ipdb; ipdb.set_trace()

if __name__ == '__main__':
    opt = docopt(__doc__)
    print(opt)
    check_opt(opt)
    main(opt)
