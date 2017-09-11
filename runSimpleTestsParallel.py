from __future__ import print_function

import argparse
import logging
import re
import subprocess
import sys
import time
from os.path import expanduser
from threading import Thread

from prettytable import PrettyTable

DEFAULT_BATCH_SIZE = 1
DEFAULT_COMMIT_SIZE = 1
DEFAULT_ROW_COUNT = 1000
DEFAULT_JVM_COUNT = 1
DEFAULT_THREAD_COUNT = 1
SCRIPT_RUNNER = expanduser("~") + "/sqlcl/bin/sql"

path_to_executable = 'java -jar ' + expanduser("~") + '/OraIngestTests/SimpleOraTest.jar'
runCommand = "{path_to_command} -u {user_name} -p {pass_word} -cs {connect_string} -bs {batch_size} -cf {commit_size} -rc {row_count} -tc {thread_count}"

results = []
process_results = []


def print_results(results, description):
    cols = description
    table = PrettyTable(cols)
    table.align = 'r'
    for row in results:
        table.add_row(row)
    print(table)


def set_logging(level):
    logger = logging.getLogger()
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(levelname)s[%(asctime)s]%(module)s:%(funcName)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def run_script(script_name):
    runcommand = '{} /nolog @{}'.format(SCRIPT_RUNNER, script_name)
    logging.debug("Command to execute : {}".format(runcommand))
    p = subprocess.Popen(runcommand, shell=True, stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    print("Output from script run : \n")
    print(output)


def executeCommand(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    s = re.findall("(Rows Inserted per sec[\s]*)([0-9,]*)", output.decode("utf-8"))
    t = re.findall("(Actual Rows Generated[\s]*)([0-9,]*)", output.decode("utf-8"))
    c = re.findall("(Connection Time[\s]*)([0-9.]*)", output.decode("utf-8"))
    i = re.findall("(Data Generation Time[\s]*)([0-9.]*)", output.decode("utf-8"))
    rows_processed = int(s[0][1].replace(',', ''))
    rows_inserted = int(t[0][1].replace(',', ''))
    connection_time = float(c[0][1])
    insertion_time = float(i[0][1])
    process_results.append((connection_time, rows_inserted, insertion_time, rows_processed,))


def run_tests(path_to_executable, username, password, connect_string, row_counts, commit_sizes, batch_sizes, thread_counts, processes, script_name):
    threads = []
    try:
        if script_name is not None:
            run_script(script_name)
        start = time.time()
        for process in range(0, int(processes[0])):
            executeCommandString = runCommand.format(path_to_command=path_to_executable,
                                                     user_name=username,
                                                     pass_word=password,
                                                     connect_string=connect_string,
                                                     batch_size=batch_sizes[0],
                                                     commit_size=commit_sizes[0],
                                                     row_count=row_counts[0],
                                                     thread_count=thread_counts[0]
                                                     )
            logging.debug("Command to execute : {}".format(executeCommandString))
            thread = Thread(target=executeCommand, args=(executeCommandString,))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        end = time.time()
        insertion_time, connection_time, rows_inserted, rows_processed, max_insertion_time = 0, 0, 0, 0, 0
        for ct, ri, it, rp in process_results:
            insertion_time += it
            connection_time += ct
            rows_inserted += ri
            rows_processed += rp
            max_insertion_time = max(max_insertion_time, it)
        logging.debug("insertion time = {}, connection time = {}, rows_inserted = {}, rows_processed = {}, max_insertion_time = {}".format(insertion_time, connection_time, rows_inserted, rows_processed, max_insertion_time))
        results = [(processes[0],
                    int(row_counts[0]) * int(processes[0]),
                    batch_sizes[0],
                    commit_sizes[0],
                    thread_counts[0],
                    "{0:,.2f}".format(end - start),
                    "{0:,.2f}".format(insertion_time),
                    "{0:,.0f}".format(rows_inserted / max_insertion_time),)]
        description = ("JVMs Started", "Total Rows Inserted", "Batch Size", "Commit Size", "Threads", "Real Time Taken", "Total Insert Time", "Rows/sec Inserted")
        print_results(results, description)
        print("Individual JVM Results")
        description = ("Connection Time", "Rows Processed", "Insert Time", "Rows/sec Inserted",)
        print_results(process_results, description)
        logging.debug("Completed test in {}".format(end - start))

    except Exception as e:
        print("Unable to run test : {}".format(e.message), file=sys.stderr)
        logging.exception("Unable to run test")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Tests Helper')
    parser.add_argument("-u", "--username", help="username", required=True)
    parser.add_argument("-p", "--password", help="password", required=True)
    parser.add_argument("-cs", "--connectstring", help="connectstring", required=True)
    parser.add_argument("-com", "--commitsizes", help="list of commit sizes to run test with (comma seperated)")
    parser.add_argument("-bat", "--batchsizes", help="list of batch sizes to run test with (comma seperated)")
    parser.add_argument("-tc", "--threads", help="list of thread counts to run test with (default=1)", default=DEFAULT_THREAD_COUNT)
    parser.add_argument("-proc", "--processes", help="number of JVMs to run (default=1)", default=DEFAULT_JVM_COUNT)
    parser.add_argument("-rs", "--runscript", help="run script before starting tests")
    parser.add_argument("-rc", "--rowcount", help="scale/size of benchmark (default=1)", default=DEFAULT_ROW_COUNT)
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring
    script_name = args.runscript

    commit_sizes = []
    if args.commitsizes is not None:
        commit_sizes = str(args.commitsizes).split(",")
    else:
        commit_sizes = [DEFAULT_COMMIT_SIZE]

    batch_sizes = []
    if args.batchsizes is not None:
        batch_sizes = str(args.batchsizes).split(",")
    else:
        batch_sizes = [DEFAULT_BATCH_SIZE]

    process_counts = []
    if args.processes is not None:
        process_counts = str(args.processes).split(",")
    else:
        process_counts = [DEFAULT_JVM_COUNT]

    thread_counts = []
    if args.threads is not None:
        thread_counts = str(args.threads).split(",")
    else:
        thread_counts = [DEFAULT_THREAD_COUNT]

    row_counts = []
    if args.rowcount is not None:
        row_counts = str(args.rowcount).split(",")
    else:
        row_counts = [DEFAULT_ROW_COUNT]

    run_tests(path_to_executable,
              username,
              password,
              connect_string,
              row_counts=row_counts,
              commit_sizes=commit_sizes,
              batch_sizes=batch_sizes,
              thread_counts=thread_counts,
              processes=process_counts,
              script_name=script_name)
