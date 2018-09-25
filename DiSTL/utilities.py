"""
these are a series of utilites for workflow management and provenance
tracking.  The main method is runner which does the following things for
each task in the list of provided tasks

    1. check whether the task has been run before, if not, run it

    2. if the task has been run, check whether its parameters have changed,
       if they have then rerun the task

    3. check whether any of the tasks dependencies have been changed and if
       they have, rerun the task

All the information about the state of each task is stored in a state_file,
which is just a json file with the following fields where each key is a
task_label and contains the following fields

    1. method_kwds

        a dictionary (json compatable) containing the params for the
        given task

    2. task_state

        a timestamp for when the task was last run

    3. dep_states

        another dictionary where each key is a dependency label and
        the value is the task_state for that depdency

                                -----

This code mostly exists because I tried to use luigi:

    https://github.com/spotify/luigi

and I found it to add a lot of unecessary (for my usecase) abstraction.

                                -----

If you are willing to trust the following list of things, this makes
it so the scripts can be created which contains all the parameters and
run-code which defines the "one truth" for the state of an experiment:

    1. The data files used and created by the methods in the run script
       haven't been changed manually or by a different script.

       1.a The best way to handle this currently, is to have a set of
           "raw" files which may be manually currated/taken from other
           sources, which only root tasks touch

       1.b Then you can have a series of intermediate/result directories
           which contain files that are only touched by one task

    2. The underlying methods called by the run scripts haven't
       fundamentally changed without their results being rerun

    3. The underlying state of the machine/software hasn't changed much
       or doesn't impact things much

1 and 3 can be handled pretty easily, 2 probably requires more thought
"""
from datetime import datetime
import logging
import smtplib
import json
import os

def _update_state(state, task_label, method_kwds, dependencies):
    """internal function for updating the state of the current task"""

    state[task_label] = {}
    state[task_label]["method_kwds"] = method_kwds
    tstr = datetime.now().strftime("%Y%m%d%H%M%S")
    state[task_label]["task_state"] = tstr
    d_state = {dep: state[dep]["task_state"]
               for dep in dependencies}
    state[task_label]["dep_states"] = d_state
    return state


def _send_email(task_label, log):
    """internal function for sending log updates to a sepcified email"""

    from .creds import password, email


    message = """From: %s <%s>

    %s
    """ % (task_label, email, log)

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(email, password)

    server.sendmail(email, email, message)
    server.quit()


def _method_wrapper(method, method_kwds, state, log_dict, task_label,
                    dependencies, create_out_dir=True, email=True,
                    provenance_kwds=None):
    """a wrapper method handles the logging/provenance tracking/status
    for each method/task

    Parameters
    ----------
    state : dict
        current state
    task_label : str
        label for current task/method
    dependencies : list
        list of task_labels for each dependency of the current task
    method : function
        method which is actually called
    method_kwds : dict
        key-words passed to method
    create_out_dir : bool
        whether to create the output data directory if it is specified in
        method_kwds and doesn't exist
    email : bool
        whether or not to email logs
    provenance_kwds : dict or None
        key-words to pass to provenance method, this TBA

    Returns
    -------
    updated state info

    Notes
    -----
    This method does the following:

        1. create output directory if it doesn't exist
        2. init logging instance
        3. run method
        4. email log if desired
        5. record provenance info
    """

    # TODO pass logger into method

    if provenance_kwds:
        raise NotImplementedError("currently we don't do full provenance")

    # create output directory if it doesn't exist
    if "out_data_dir" in method_kwds:
        if not os.path.exists(method_kwds["out_data_dir"]):
            os.makedirs(method_kwds["out_data_dir"], exist_ok=True)

    # for tracking runtime
    t0 = datetime.now()

    # init temporary logger
    open("tmp.log", "w")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S",
                        filename="tmp.log")
    logger = logging.getLogger(task_label)

    # start log
    logger.info("%s started" % task_label)

    # email on start
    with open("tmp.log", "r") as fd:
        curr_log = fd.read()
        log_dict[task_label] = curr_log
        print("log", curr_log)
    if email:
        _send_email(task_label, curr_log)

    # run method
    method(**method_kwds)
    t1 = datetime.now()
    logger.info("%s finished" % task_label)
    logger.info("runtime: %s" % str(t1 - t0))

    # update state
    state = _update_state(state, task_label, method_kwds, dependencies)

    # email on finish
    with open("tmp.log", "r") as fd:
        curr_log = fd.read()
        log_dict[task_label] = curr_log
    if email:
        _send_email(task_label, curr_log)

    return state, log_dict


def runner(task_list, state_file, log_file, method_wrapper_kwds=None):
    """this is a method doing work-flow management/provenance tracking

    Parameters
    ----------
    task_list : list
        this is a list of tasks run in sequential order.  Each element
        contains a dict with the following elements

        1. label
            name of task
        2. method
            function to run
        3. method_kwds
            key-words to pass to method
        4. dependencies
            list of labels for other tasks which are needed for the current
            task to run

    state_file : str
        location of file where state info for each task is stored
    log_file : str
        location of log file
    method_wrapper_kwds : dict or None
        key-words passed to _method_wrapper

    Returns
    -------
    None

    Updates
    -------
    state_file, log_file
    """

    # create method_wrapper_kwds if None provided
    if method_wrapper_kwds is None:
        method_wrapper_kwds = {}

    # if the state file doesn't exist, init
    if not os.path.exists(state_file):
        state = {}
        with open(state_file, "w") as fd:
            json.dump(state, fd)
    # if the log file doesn't exist, init
    if not os.path.exists(log_file):
        log_dict = {}
        with open(log_file, "w") as fd:
            json.dump(log_dict, fd)

    # iterate over steps
    for task in task_list:

        # load current state and log_dict
        with open(state_file, "r") as fd:
            state = json.load(fd)
        with open(log_file, "r") as fd:
            log_dict = json.load(fd)

        # extract variables for clarity
        task_label = task["label"]
        method = task["method"]
        method_kwds = task["method_kwds"]
        dependencies = task["dependencies"]

        # first check that all the dependencies of the task have run
        for dep in dependencies:
            if dep not in state:
                ts = ("dependency: %s for task: %s hasn't run yet" %
                      (dep, task_label))
                raise ValueError(ts)

        # next check if the task has been run before
        if task_label not in state:
            res = _method_wrapper(method, method_kwds, state, log_dict,
                                  task_label, dependencies,
                                  **method_wrapper_kwds)
            state, log_dict = res

        # if the task has been run before, check whether the params have
        # changed
        elif state[task_label]["method_kwds"] != method_kwds:
            res = _method_wrapper(method, method_kwds, state, log_dict,
                                  task_label, dependencies,
                                  **method_wrapper_kwds)
            state, log_dict = res

        # if the params haven't changed, check whether the depencies have
        # changed
        else:
            chng = False
            for dep in dependencies:
                if (state[task_label]["dep_states"][dep] !=
                    state[dep]["task_state"]):
                    chng = True
            if chng:
                res = _method_wrapper(method, method_kwds, state, log_dict,
                                      task_label, dependencies,
                                      **method_wrapper_kwds)
                state, log_dict = res

        # write current state
        with open(state_file, "w") as fd:
            json.dump(state, fd)

        # write current log
        with open(log_file, "w") as fd:
            json.dump(log_dict, fd)
