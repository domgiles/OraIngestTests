from __future__ import print_function

import argparse
import logging
import subprocess
import sys

import os
import re
from prettytable import PrettyTable
from tqdm import tqdm

runCommand = "{path_to_command}/datagenerator -c {config_file} -u {user_name} -p {pass_word} -cs {connect_string} -bs {batch_size} -commit {commit_size} -scale {scale} -db -cl -nodrop -noddl -tc {threads} -trunc -async"
DEFAULT_BATCH_SIZE=100
DEFAULT_COMMIT_SIZE=100
DEFAULT_SCALE = 1
DEFAUT_IMAGE_SIZE=2000
DEFAULT_THREAD_COUNT=1

def print_results(results, *description):
    cols = description + ("Rows/Sec",)
    table = PrettyTable(cols)
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


def run_tests(config, username, password, connect_string, commit_sizes, batch_sizes, threads, scale):
    results = []
    path = os.path.dirname(os.path.realpath(sys.argv[0]))
    logging.debug("\nconfig : {}\nusername : {}\npassword : {}\nconnect string : {}\ncommit_sizes : {}\nbatch_sizes : {}\npath : {}".format(config, username, password, connect_string, commit_sizes, batch_sizes, path))

    try:
        with tqdm(desc="Tests Run", total=len(commit_sizes)*len(batch_sizes)) as pbar:
            for commit_size in commit_sizes:
                for batch_size in batch_sizes:
                    execute = runCommand.format(path_to_command=path,
                                                config_file=config,
                                                user_name=username,
                                                pass_word=password,
                                                connect_string=connect_string,
                                                batch_size=batch_size,
                                                commit_size=commit_size,
                                                threads=threads,
                                                scale=scale)
                    logging.debug("Command to execute : {}".format(execute))
                    p = subprocess.Popen(execute, stdout=subprocess.PIPE, shell=True)
                    (output, err) = p.communicate()
                    s = re.findall("(Rows Inserted per sec[\s]*)([0-9,]*)", output)
                    rows_processed = s[0][1]
                    results.append((commit_size, batch_size, rows_processed))
                    pbar.update(1)
        print_results(results, "Commit Size", "Batch Size")
    except Exception as e:
        print("Unable to run test : {}".format(e.message), file=sys.stderr)
        logging.exception("Unable to run test")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Tests Helper')
    parser.add_argument("-u", "--username", help="username", required=True)
    parser.add_argument("-p", "--password", help="password", required=True)
    parser.add_argument("-cs", "--connectstring", help="connectstring", required=True)
    parser.add_argument("-st", "--schematype", help="run the tests against a relational or document schema", choices=['relational', 'document'], default="relational")
    parser.add_argument("-com", "--commitsizes", help="list of commit sizes to run test with (comma seperated)")
    parser.add_argument("-bat", "--batchsizes", help="list of batch sizes to run test with (comma seperated)")
    parser.add_argument("-tc", "--threads", help="number of threads to run test with (default=1)", default=DEFAULT_THREAD_COUNT)
    parser.add_argument("-scale", "-scale", help="scale/size of benchmark (default=1)", default=DEFAULT_SCALE)
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring
    test_type = args.schematype
    thread_count=args.threads
    scale=args.scale

    commit_sizes = []
    if args.commitsizes != None:
        commit_sizes = args.commitsizes.split(",")

    batch_sizes = []
    if args.batchsizes != None:
        batch_sizes = args.batchsizes.split(",")

    relational_config = "anpr_relational.xml"
    document_config = "anpr.xml"
    config = None

    if test_type == "relational":
        config = relational_config
    else:
        config = document_config

    # run_batch_tests(config, username, password, connect_string, batch_sizes, threads=thread_count, scale=scale)
    run_tests(config, username, password, connect_string, commit_sizes=commit_sizes, batch_sizes=batch_sizes, threads=thread_count, scale=scale)
    # run_commit_tests(config, username, password, connect_string, commit_sizes)
