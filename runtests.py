from __future__ import print_function

import argparse
import logging
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from os.path import expanduser
from threading import Thread

from prettytable import PrettyTable
from tqdm import tqdm

runCommand = "{path_to_command} -c {config_file} -u {user_name} -p {pass_word} -cs {connect_string} -bs {batch_size} -commit {commit_size} -scale {scale} -db -cl -nodrop -noddl -tc {threads} {async}"
SCRIPT_RUNNER = expanduser("~") + "/sqlcl/bin/sql"
DEFAULT_BATCH_SIZE = 100
DEFAULT_COMMIT_SIZE = 100
DEFAULT_SCALE = 1
DEFAUT_IMAGE_MULTIPLIER = 1
DEFAULT_THREAD_COUNT = 1
DEFAULT_DATAGEN_LOCATION = expanduser("~") + "/datagenerator/bin/datagenerator"
DEFAULT_REL_CONFIG = "anpr_relationalv2.xml"
DEFAULT_DOC_CONFIG = "anpr_documentv2.xml"
DEFAULT_SIMPLE_CONFIG = "anpr_simple.xml"
DEFAULT_JVM_COUNT = 1

process_results = []
results = []


def timingtoseconds(timingstring):
    delta = (datetime.strptime(timingstring, "%H:%M:%S.%f") - datetime(1900, 1, 1))
    seconds = float(delta.seconds) + (float(delta.microseconds) / 1000000)
    return seconds


def print_results(results, *description):
    cols = description
    table = PrettyTable(cols)
    table.align = 'r'
    for row in results:
        table.add_row(row)
    print(table)

def run_script(script_name, supress_script_output):
    print ("Running script {}".format(script_name))
    runcommand = '{} /nolog @{}'.format(SCRIPT_RUNNER, script_name)
    logging.debug("Command to execute : {}".format(runcommand))
    p = subprocess.Popen(runcommand, shell=True, stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    if supress_script_output:
        print("Output from script run : \n")
        print(output)


def executeCommand(command):
    rows_processed, rows_inserted, connection_time, insertion_time = 0, 0, 0, 0
    logging.debug("Command to execute : {}".format(command))
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    s = re.findall("(Rows Inserted per sec[\s]*)([0-9,]*)", output.decode("utf-8"))
    t = re.findall("(Actual Rows Generated[\s]*)([0-9,]*)", output.decode("utf-8"))
    c = re.findall("(Connection Time[\s]*)([0-9.:]*)", output.decode("utf-8"))
    i = re.findall("(Data Generation Time[\s]*)([0-9.:]*)", output.decode("utf-8"))
    if (len(s) != 0):
        rows_processed = int(s[0][1].replace(',', ''))
    if (len(t) != 0):
        rows_inserted = int(t[0][1].replace(',', ''))
    if (len(c) != 0):
        connection_time = timingtoseconds(c[0][1])
    if (len(i) != 0):
        insertion_time = timingtoseconds(i[0][1])
    process_results.append((connection_time, rows_inserted, insertion_time, rows_processed,))


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


def run_tests(path_to_executable, config, username, password, connect_string, commit_sizes, batch_sizes, image_multipliers, thread_counts, scale, async, test_type, processes, jvm_display, script_name, supress_script_output):
    logging.debug("\nconfig : {}\nusername : {}\npassword : {}\nconnect string : {}\ncommit_sizes : {}\nbatch_sizes : {}\npath : {}\nscale : {}\nasync : {}\nimage_sizes : {}\nthread_counts : {}\njvms started : {}".format(
        config, username, password, connect_string, commit_sizes, batch_sizes, path, scale, async, image_multipliers, thread_counts, processes[0]))

    try:
        if script_name is not None:
            run_script(script_name, supress_script_output)
        with tqdm(desc="Tests Run", total=len(commit_sizes) * len(batch_sizes) * len(image_multipliers) * len(thread_counts)) as pbar:
            for commit_size in commit_sizes:
                for batch_size in batch_sizes:
                    for image_multiplier in image_multipliers:
                        for thread_count in thread_counts:
                            if test_type != 'simple':
                                new_config = changeImageSize(config, image_multiplier)
                            else:
                                new_config = config
                            my_threads = []
                            start = time.time()
                            for process in range(0, int(processes[0])):
                                executeCommandString = runCommand.format(path_to_command=path_to_executable,
                                                                         config_file=new_config,
                                                                         user_name=username,
                                                                         pass_word=password,
                                                                         connect_string=connect_string,
                                                                         batch_size=batch_size,
                                                                         commit_size=commit_size,
                                                                         threads=thread_count,
                                                                         scale=scale,
                                                                         async=('-async' if async else ''))

                                thread = Thread(target=executeCommand, args=(executeCommandString,))
                                my_threads.append(thread)

                            for thread in my_threads:
                                thread.start()
                            for thread in my_threads:
                                thread.join()
                            end = time.time()
                            insertion_time, connection_time, rows_inserted, rows_processed, max_insertion_time = 0, 0, 0, 0, 0
                            for ct, ri, it, rp in process_results:
                                insertion_time += it
                                connection_time += ct
                                rows_inserted += ri
                                rows_processed += rp
                                max_insertion_time = max(max_insertion_time, it)
                            logging.debug(
                                "insertion time = {}, connection time = {}, rows_inserted = {}, rows_processed = {}, max_insertion_time = {}".format(insertion_time, connection_time, rows_inserted, rows_processed, max_insertion_time))

                            results.append((processes[0],
                                            thread_count,
                                            commit_size,
                                            batch_size,
                                            int(0 if test_type == 'simple' else image_multiplier) * 100, async,
                                            rows_inserted,
                                            "{0:,.2f}".format(end - start),
                                            "{0:,.2f}".format(insertion_time),
                                            "{0:,.0f}".format((rows_inserted / max_insertion_time) if max_insertion_time != 0 else 0),))
                            if test_type != 'simple':
                                os.remove(new_config)
                            pbar.update(1)
                            if jvm_display:
                                print_results(process_results, "Connection Time", "Rows Processed", "Insert Time", "Rows/sec Inserted")
                            del process_results[:]
        print_results(results, "JVMs Started", "Thread Count", "Commit Size", "Batch Size", "Image Size", "Async", "Total Rows Inserted", "Real Time Taken", "Total Insert Time", "Rows/sec Inserted")
    except Exception as e:
        print("Unable to run test : {}".format(e.message), file=sys.stderr)
        logging.exception("Unable to run test")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Tests Helper')
    parser.add_argument("-u", "--username", help="username", required=True)
    parser.add_argument("-p", "--password", help="password", required=True)
    parser.add_argument("-cs", "--connectstring", help="connectstring", required=True)
    parser.add_argument("-st", "--schematype", help="run the tests against a relational, document or simple schema", choices=['relational', 'document', 'simple'], default="relational")
    parser.add_argument("-com", "--commitsizes", help="list of commit sizes to run test with (comma seperated)")
    parser.add_argument("-bat", "--batchsizes", help="list of batch sizes to run test with (comma seperated)")
    parser.add_argument("-tc", "--threads", help="list of thread counts to run test with (default=1)", default=DEFAULT_THREAD_COUNT)
    parser.add_argument("-rs", "--runscript", help="run script before starting tests")
    parser.add_argument("-scale", "-scale", help="scale/size of benchmark (default=1)", default=DEFAULT_SCALE)
    parser.add_argument("-dgl", "--dglocation", help="path to the datagenerator executable", default=DEFAULT_DATAGEN_LOCATION)
    parser.add_argument("-im", "--imagemultipliers", help="list of image multipliers (comma seperated, default = 1)", default=DEFAUT_IMAGE_MULTIPLIER)
    parser.add_argument("-proc", "--processes", help="number of JVMs to run (default=1)", default=DEFAULT_JVM_COUNT)
    parser.add_argument("-pd", "--procdisplay", help="show JVM results (default=false)", dest='jvm_display', action='store_true')
    parser.add_argument("-debug", help="output debug to stdout", dest='debug_on', action='store_true')
    parser.add_argument("-async", help="Use async transactions ", dest='async_on', action='store_true')
    parser.add_argument("-ss", "-suppress", help="Suppress script output", dest='async_on', action='store_true')

    args = parser.parse_args()

    if args.debug_on:
        set_logging(level=logging.DEBUG)

    username = args.username
    password = args.password
    connect_string = args.connectstring
    test_type = args.schematype
    scale = args.scale
    dg_location = args.dglocation
    script_name = args.runscript

    logging.debug("Display jvm info : {}".format(args.jvm_display))

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

    process_counts = []
    if args.processes is not None:
        process_counts = str(args.processes).split(",")
    else:
        process_counts = [DEFAULT_JVM_COUNT]

    config = None
    path = os.path.dirname(os.path.realpath(sys.argv[0]))
    if test_type == "relational":
        config = "{0}/{1}".format(path, DEFAULT_REL_CONFIG)
    elif test_type == 'document':
        config = "{0}/{1}".format(path, DEFAULT_DOC_CONFIG)
    else:
        config = "{0}/{1}".format(path, DEFAULT_SIMPLE_CONFIG)

    run_tests(dg_location, config,
              username,
              password,
              connect_string,
              commit_sizes=commit_sizes,
              batch_sizes=batch_sizes,
              image_multipliers=image_multipliers,
              thread_counts=thread_counts,
              scale=scale,
              async=args.async_on,
              test_type=test_type,
              processes=process_counts,
              jvm_display=args.jvm_display,
              script_name=script_name,
              supress_script_output=args.suppress)
