from __future__ import print_function

import argparse
import logging
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from os.path import expanduser

from prettytable import PrettyTable
from tqdm import tqdm

runCommand = "{path_to_command} -c {config_file} -u {user_name} -p {pass_word} -cs {connect_string} -bs {batch_size} -commit {commit_size} -scale {scale} -db -cl -nodrop -noddl -tc {threads} -trunc {async}"
DEFAULT_BATCH_SIZE = 100
DEFAULT_COMMIT_SIZE = 100
DEFAULT_SCALE = 1
DEFAUT_IMAGE_MULTIPLIER = 1
DEFAULT_THREAD_COUNT = 1
DEFAULT_DATAGEN_LOCATION = expanduser("~") + "/datagenerator/bin/datagenerator"
DEFAULT_REL_CONFIG = "anpr_relationalv2.xml"
DEFAULT_DOC_CONFIG = "anpr_documentv2.xml"


def print_results(results, *description):
    cols = description + ("Rows/Sec",)
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


def changeImageSize(configfile, new_size):
    tree = ET.ElementTree(file=configfile)

    root = tree.getroot()

    ns = "{http://www.domincgiles.com/datagen}"

    minimum_element = root.find(".//{}EnumerationGenerator[{}id='IMAGE_GENERATOR']/{}MinimumRepetitions".format(ns, ns, ns))
    maximum_element = root.find(".//{}EnumerationGenerator[{}id='IMAGE_GENERATOR']/{}MaximumRepetitions".format(ns, ns, ns))

    minimum_element.text = str(new_size)
    maximum_element.text = str(new_size)

    new_file = '{}/copy_{}'.format(os.path.dirname(configfile), os.path.basename(configfile))
    tree.write(new_file)
    return new_file


def run_tests(path_to_executable, config, username, password, connect_string, commit_sizes, batch_sizes, image_multipliers, threads, scale, async):
    results = []

    logging.debug("\nconfig : {}\nusername : {}\npassword : {}\nconnect string : {}\ncommit_sizes : {}\nbatch_sizes : {}\npath : {}\nscale : {}\nasync : {}\nimage_sizes : {}\nthread_counts : {}".format(
        config, username, password, connect_string, commit_sizes, batch_sizes, path, scale, async, image_multipliers, threads))

    try:
        with tqdm(desc="Tests Run", total=len(commit_sizes) * len(batch_sizes) * len(image_multipliers) * len(threads)) as pbar:
            for commit_size in commit_sizes:
                for batch_size in batch_sizes:
                    for image_multiplier in image_multipliers:
                        for thread_count in threads:
                            new_config = changeImageSize(config, image_multiplier)
                            execute = runCommand.format(path_to_command=path_to_executable,
                                                        config_file=new_config,
                                                        user_name=username,
                                                        pass_word=password,
                                                        connect_string=connect_string,
                                                        batch_size=batch_size,
                                                        commit_size=commit_size,
                                                        threads=thread_count,
                                                        scale=scale,
                                                        async=('-async' if async else ''))
                            logging.debug("Command to execute : {}".format(execute))
                            p = subprocess.Popen(execute, stdout=subprocess.PIPE, shell=True)
                            (output, err) = p.communicate()
                            s = re.findall("(Rows Inserted per sec[\s]*)([0-9,]*)", output.decode("utf-8"))
                            t = re.findall("(Actual Rows Generated[\s]*)([0-9,]*)", output.decode("utf-8"))
                            u = re.findall("(Data Generation Time[\s]*)([0-9:.]*)", output.decode("utf-8"))
                            rows_processed = s[0][1]
                            rows_inserted = t[0][1]
                            time_taken = u[0][1]
                            results.append((thread_count, commit_size, batch_size, int(image_multiplier) * 100, async, rows_inserted, time_taken, rows_processed))
                            os.remove(new_config)
                            pbar.update(1)
        print_results(results, "Thread Count", "Commit Size", "Batch Size", "Image Size", "Async", "Total Rows Inserted", "Time Taken")
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
    parser.add_argument("-tc", "--threads", help="list of thread counts to run test with (default=1)", default=DEFAULT_THREAD_COUNT)
    parser.add_argument("-scale", "-scale", help="scale/size of benchmark (default=1)", default=DEFAULT_SCALE)
    parser.add_argument("-dgl", "--dglocation", help="path to the datagenerator executable", default=DEFAULT_DATAGEN_LOCATION)
    parser.add_argument("-im", "--imagemultipliers", help="list of image multipliers (comma seperated, default = 1)", default=DEFAUT_IMAGE_MULTIPLIER)
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')
    parser.add_argument("-async", help="Use ", dest='async_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring
    test_type = args.schematype
    scale = args.scale
    dg_location = args.dglocation

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

    image_multipliers = []
    if args.imagemultipliers is not None:
        image_multipliers = str(args.imagemultipliers).split(",")
    else:
        image_multipliers = [DEFAUT_IMAGE_MULTIPLIER]

    thread_counts = []
    if args.threads is not None:
        thread_counts = str(args.threads).split(",")
    else:
        thread_counts = [DEFAULT_THREAD_COUNT]

    config = None
    path = os.path.dirname(os.path.realpath(sys.argv[0]))
    if test_type == "relational":
        config = "{0}/{1}".format(path, DEFAULT_REL_CONFIG)
    else:
        config = "{0}/{1}".format(path, DEFAULT_DOC_CONFIG)

    run_tests(dg_location, config, username, password, connect_string, commit_sizes=commit_sizes, batch_sizes=batch_sizes, image_multipliers=image_multipliers, threads=thread_counts, scale=scale, async=args.async_on)
