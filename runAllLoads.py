from __future__ import print_function

import argparse
import logging
import os
import sys
import subprocess
import select
import threading
from threading import Thread

path_to_executable = '/Users/dgiles/sqlcl/bin/sql'
runCommand = "{path_to_command} {user_name}/{pass_word}@{connect_string} << EOF\nselect 1 from dual\nexit;\nEOF"
# runCommand = "{path_to_command} {user_name}/{pass_word}@{connect_string} control={control_file}"


def set_logging(level):
    logger = logging.getLogger()
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(levelname)s[%(asctime)s]%(module)s:%(funcName)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def executeCommand(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    logging.debug(output)


def run_tests(path_to_executable, username, password, connect_string, control_files):
    results = []

    logging.debug("\nusername : {}\npassword : {}\nconnect string : {}\ndirecotries : {}\n".format(
        username, password, connect_string, control_files))
    threads = []
    try:
        for control_file in control_files:
            executeCommandString = runCommand.format(path_to_command=path_to_executable,
                                        user_name=username,
                                        pass_word=password,
                                        connect_string=connect_string,
                                        control_file=control_file)
            logging.debug("Command to execute : {}".format(executeCommandString))
            thread = Thread(target=executeCommand, args=(executeCommandString,))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        logging.debug("Finished all threads")

    except Exception as e:
        print("Unable to run test : {}".format(e.message), file=sys.stderr)
        logging.exception("Unable to run test")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Tests Helper')
    parser.add_argument("-u", "--username", help="username", required=True)
    parser.add_argument("-p", "--password", help="password", required=True)
    parser.add_argument("-cs", "--connectstring", help="connectstring", required=True)
    parser.add_argument("-d", "--directory", help="directory containing control files", required=True)
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring

    controlfiles = []

    for file in os.listdir(args.directory):
        if file.endswith(".ctl"):
            controlfiles.append(os.path.join(args.directory, file))

    run_tests(path_to_executable, username, password, connect_string, controlfiles)
