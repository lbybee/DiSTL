"""
these are a series of utilites for workflow management and provenance
tracking.  The main method is runner which does the following things for
each task in the list of provided tasks

1. check whether the task has been run before, if not, run it

2. if the task has been run, check whether its parameters have changed, if
   they have then rerun the task

3. check whether any of the tasks dependencies have been changed and if they
   rerun the task

All the information about the state of each task is stored in a state_file

This mostly exists because I tried to use luigi:

    https://github.com/spotify/luigi

and I found it to add a lot of unecessary (for my usecase) abstraction.
"""
from datetime import datetime
import json
import os


def _update_state(state, task_label, method_kwds, dependencies):
    """updates the current state of the current task"""

    state[task_label] = {}
    state[task_label]["method_kwds"] = method_kwds
    tstr = datetime.now().strftime("%Y%m%d%H%M%S")
    state[task_label]["task_state"] = tstr
    d_state = {dep: state[dep]["task_state"]
               for dep in dependencies}
    state[task_label]["dep_states"] = d_state
    return state


def runner(task_list, state_file):
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

    Returns
    -------
    None

    Updates
    -------
    state_file
    """

    # if the state file doesn't exist, init
    if not os.path.exists(state_file):
        state = {}
        with open(state_file, "w") as fd:
            json.dump(state, fd)

    # iterate over steps
    for task in task_list:

        # load current state
        with open(state_file, "r") as fd:
            state = json.load(fd)

        # extract variables for clarity
        task_label = task["label"]
        method = task["method"]
        method_kwds = task["method_kwds"]
        dependencies = task["dependencies"]

        # TODO maybe handle this externally
        # create output directory if it doesn't exist
        if "out_data_dir" in method_kwds:
            if not os.path.exists(method_kwds["out_data_dir"]):
                os.makedirs(method_kwds["out_data_dir"], exist_ok=True)

        # first check that all the dependencies of the task have run
        for dep in dependencies:
            if dep not in state:
                ts = ("dependency: %s for task: %s hasn't run yet" %
                      (dep, task_label))
                raise ValueError(ts)

        # next check if the task has been run before
        if task_label not in state:
            method(**method_kwds)
            state = _update_state(state, task_label, method_kwds,
                                  dependencies)

        # if the task has been run before, check whether the params have
        # changed
        elif state[task_label]["method_kwds"] != method_kwds:
            method(**method_kwds)
            state = _update_state(state, task_label, method_kwds,
                                  dependencies)

        # if the params haven't changed, check whether the depencies have
        # changed
        else:
            chng = False
            for dep in dependencies:
                if (state[task_label]["dep_states"][dep] !=
                    state[dep]["task_state"]):
                    chng = True
            if chng:
                method(**method_kwds)
                state = _update_state(state, task_label, method_kwds,
                                      dependencies)

        # write current state
        with open(state_file, "w") as fd:
            json.dump(state, fd)
