from __future__ import print_function

import argparse
import logging
import subprocess
import sys
import xml.etree.ElementTree as ET

import os
import re
from os.path import expanduser
from prettytable import PrettyTable
from tqdm import tqdm

runCommand = "{path_to_command} -c {config_file} -u {user_name} -p {pass_word} -cs {connect_string} -bs {batch_size} -commit {commit_size} -scale {scale} -db -cl -nodrop -noddl -tc {threads} -trunc -async"
DEFAULT_BATCH_SIZE = 100
DEFAULT_COMMIT_SIZE = 100
DEFAULT_SCALE = 1
DEFAUT_IMAGE_SIZE = 2000
DEFAULT_THREAD_COUNT = 1
DEFAULT_DATAGEN_LOCATION = expanduser("~") + "/datagenerator/bin/datagenerator"
DEFAULT_REL_CONFIG = "anpr_relational.xml"
DEFAULT_DOC_CONFIG = "anpr_document.xml"


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


def changeImageSize(configfile, new_size):
    tree = ET.ElementTree(file=configfile)

    root = tree.getroot()

    ns = "{http://www.domincgiles.com/datagen}"

    minimum_element = root.find(".//{}CharacterGenerator[{}id='IMAGE_GENERATOR']/{}MinimumSize".format(ns, ns, ns))
    maximum_element = root.find(".//{}CharacterGenerator[{}id='IMAGE_GENERATOR']/{}MaximumSize".format(ns, ns, ns))

    minimum_element.text = str(new_size)
    maximum_element.text = str(new_size)

    new_file = '{}/copy_{}'.format(os.path.dirname(configfile), os.path.basename(configfile))
    tree.write(new_file)
    return new_file


def run_tests(path_to_executable, config, username, password, connect_string, commit_sizes, batch_sizes, threads, scale):
    results = []

    logging.debug("\nconfig : {}\nusername : {}\npassword : {}\nconnect string : {}\ncommit_sizes : {}\nbatch_sizes : {}\npath : {}".format(config, username, password, connect_string, commit_sizes, batch_sizes, path))

    try:
        with tqdm(desc="Tests Run", total=len(commit_sizes) * len(batch_sizes)) as pbar:
            for commit_size in commit_sizes:
                for batch_size in batch_sizes:
                    execute = runCommand.format(path_to_command=path_to_executable,
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
                    s = re.findall("(Rows Inserted per sec[\s]*)([0-9,]*)", output.decode("utf-8"))
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
    parser.add_argument("-dgl", "--dglocation", help="path to the datagenerator executable", default=DEFAULT_DATAGEN_LOCATION)
    parser.add_argument("-is", "--imagesize", help="size of image file created for each record", default=DEFAUT_IMAGE_SIZE)
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')
    parser.add_argument("-async", help="Use ", dest='debug_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring
    test_type = args.schematype
    thread_count = args.threads
    scale = args.scale
    dg_location = args.dglocation
    image_size = args.imagesize

    commit_sizes = []
    if args.commitsizes is not None:
        commit_sizes = args.commitsizes.split(",")

    batch_sizes = []
    if args.batchsizes is not None:
        batch_sizes = args.batchsizes.split(",")

    config = None
    path = os.path.dirname(os.path.realpath(sys.argv[0]))
    if test_type == "relational":
        config = "{0}/{1}".format(path, DEFAULT_REL_CONFIG)
    else:
        config = "{0}/{1}".format(path, DEFAULT_DOC_CONFIG)

    new_config = changeImageSize(config, image_size)

    run_tests(dg_location, new_config, username, password, connect_string, commit_sizes=commit_sizes, batch_sizes=batch_sizes, threads=thread_count, scale=scale)

    os.remove(new_config)
