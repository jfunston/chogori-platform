'''
MIT License

Copyright (c) 2021 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

from parse import Runnable
import locals
import pickle


def load_state(pickle_filename):
    assignment = None
    try:
        state_p = open(pickle_filename, 'rb')
        assignment = pickle.load(state_p)
        state_p.close()
    except:
        pass

    if not assignment:
        assignment = []
        for node in locals.host_nodes:
            assignment.append([node, None])

    return assignment


def save_state(pickle_filename, assignment):
    state_p = open(pickle_filename, 'wb')
    pickle.dump(assignment, state_p)
    state_p.close()


def add_runnable(assignment, runnable: Runnable):
    # First check for free assignment
    assigned = None
    for node in assignment:
        if assignment[0].config == runnable.target_config and assignment[1] is None and \
                len(assignment[0].cores) <= runnable.num_cpus:
            assigned = node
            break
    if assigned is None:
        raise RuntimeError(f"No free hosts for {runnable}")

    # Second check for cluster prerequisites (e.g. CPO must be parsed before nodepool)
    can_assign = False
    # CPO and TSO are standalone with no prerequisites
    if runnable.component == "cpo" or runnable.component == "tso":
        can_assign = True
    # TODO others
    if not can_assign:
        raise RuntimeError(f"Cluster prerequisites are not met for {runnable}")
