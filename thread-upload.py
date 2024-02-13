#!/usr/bin/env python
"""thread-upload.py
Box Thread upload tool

Usage:
  thread-upload.py upload <JWT-FILE> <FILE> [--user=<BOX-USER-ID>] [--folder=<FOLDER-ID>] [--file=<FILE-ID>] [--thread=<THREAD-NUM>] 
  thread-upload.py benchmark <JWT-FILE> <SIZE> [--user=<BOX-USER-ID>] [--folder=<FOLDER-ID>] [--thread=<THREAD-NUM>]

Options:
  <JWT-FILE>                  JWT file.
  <FILE>                      File to upload.
  <SIZE>                      File size to upload(bytes). Specify 2000001 or more for the size.
  --user=<BOX-USER-ID>        Box user id. If there is no specification, the service account ID will be used.
  --folder=<FOLDER-ID>        Box folder id. [default: 0].
  --file=<FILE-ID>            Box file id. Updates a file with an already existing file ID; cannot be used in conjunction with FOLDER-ID.
  --thread=<THREAD-NUM>       Number of thread [default: 4].

  -h --help                   Show this screen.
  --version                   Show version.
"""
import hashlib
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from boxsdk import Client, JWTAuth
from docopt import docopt
from loguru import logger


def check_opt(opt):
    # Check if JWT file exists and is readable
    if not os.path.isfile(opt['<JWT-FILE>']) or not os.access(opt['<JWT-FILE>'], os.R_OK):
        print(f"Error: JWT file '{opt['<JWT-FILE>']}' does not exist or is not readable.")
        sys.exit(1)

    # If the user ID is specified, check if it is a number
    if opt['--user']:
        try:
            int(opt['--user'])
        except Exception as e:
            print(f"Error: User id '{opt['--user']}' is not a number.")
            sys.exit(1)

    # Check if the thread number is a number
    try:
        int(opt['--thread'])
    except Exception as e:
        print(f"Error: thread number '{opt['--thread']}' is not a number.")
        sys.exit(1)

    # Check if Folder ID is a number
    try:
        int(opt['--folder'])
    except Exception as e:
        print(f"Error: Folder id '{opt['--folder']}' is not a number.")
        sys.exit(1)

    if opt['upload']:
        # Chekc File ID a numbers
        if opt['--file']:
            try:
                int(opt['--file'])
            except Exception as e:
                print(f"Error: File id '{opt['--file']}' is not a number.")
                sys.exit(1)

        # Check if file exists and is readable
        if not os.path.isfile(opt['<FILE>']) or not os.access(opt['<FILE>'], os.R_OK):
            print(f"Error: File '{opt['<FILE>']}' does not exist or is not readable.")
            sys.exit(1)

        _folder_id = None if opt['--folder'] == '0' else opt['--folder']

        if _folder_id and opt['--file']:
            print(f"Error: Cannot specify both FILE-ID and FOLDER-ID.")
            sys.exit(1)

    if opt["benchmark"]:
        try:
            int(opt['<SIZE>'])
        except Exception as e:
            print(f"Error: Size '{opt['<SIZE>']}' is not a number.")
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
    auth = None
    try:
        auth = JWTAuth.from_settings_file(jwt_file, user=user)
        return auth
    except Exception as e:
        print(f"Error: JWT file '{jwt_file}' is not valid.")
        sys.exit(1)


def get_folder(client, folder_id):
    try:
        folder = client.folder(folder_id).get()
        return folder
    except Exception as e:
        print(f"Error: Folder id '{folder_id}' not found in box.")
        sys.exit(1)


def get_file(client, file_id):
    ...
    try:
        file = client.file(file_id).get()
        return file
    except Exception as e:
        print(f"Error: Folder id '{file_id}' not found in box.")
        sys.exit(1)


def benchmark_main(opt, client):
    def _upload_thread(opt, i, upload_session, sha1):
        total_size = int(opt["<SIZE>"])
        part_size = upload_session.part_size
        offset = i * part_size

        # If the last part is smaller than the part size, adjust the part size
        if part_size + offset > total_size:
            part_size = total_size - offset

        bytes = b"a" * part_size

        logger.info(f"ℹ️ Upload Thread [{i}] Start upload. offset: {offset} "
                    f"part_size: {part_size} real_binary_size: {len(bytes)} total_size: {total_size}")

        error_count = 0
        while True:
            try:
                upload_session.upload_part_bytes(bytes, offset, total_size)
                break
            except Exception as e:
                if error_count >= 3:
                    logger.error(f"❌ Upload Thread [{i}] Upload failed!. {e}. Exit thread.")
                    return

                error_count += 1
                logger.error(f"❌ Upload Thread [{i}] Upload failed!. {e}")
                time.sleep(i * 1)

        logger.info(f"ℹ️ Upload Thread [{i}] Upload completed.")
        sha1.update(bytes)


    folder = get_folder(client, opt['--folder'])

    # Create session
    upload_session = folder.create_upload_session(
        file_size=int(opt['<SIZE>']),
        file_name=f"upload-bench-{datetime.now().isoformat()}.bench"
    )
    logger.info(f"ℹ️ Upload Session id: {upload_session.id} part_size: {upload_session.part_size} total_parts: {upload_session.total_parts}")

    # If the number of threads is higher than the number of parts, adjust the number of parts
    thread_num = int(opt['--thread'])
    if thread_num > upload_session.total_parts:
        thread_num = upload_session.total_parts
        logger.info("🤔 The number of threads is higher than the number of parts, adjust the number of parts. thread_num: {thread_num}")

    sha1 = hashlib.sha1()
    with ThreadPoolExecutor(max_workers=int(opt['--thread'])) as executor:
        for i in range(upload_session.total_parts):
            offset = i * upload_session.part_size

            executor.submit(
                _upload_thread, opt, i, upload_session, sha1)

    uploaded_file = upload_session.commit(sha1.digest())
    logger.info(f"ℹ️ Upload completed. file_id: {uploaded_file.id}")

def upload_main(opt, client):
    ...


def main(opt):
    # Get Client
    client = create_client(opt['<JWT-FILE>'], user_id= opt['--user'])

    if opt["benchmark"]:
        benchmark_main(opt, client)

    if opt["upload"]:
        upload_main(opt, client)

if __name__ == '__main__':
    opt = docopt(__doc__)

    check_opt(opt)
    main(opt)
