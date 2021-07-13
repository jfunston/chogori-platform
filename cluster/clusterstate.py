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

    if assignment is None:
        assignment = []
        for node in locals.host_nodes:
            assignment.append([node, None])

    return assignment


def save_state(pickle_filename, assignment):
    state_p = open(pickle_filename, 'wb')
    pickle.dump(assignment, state_p)
    state_p.close()


def replace_program_arg(runnable: Runnable, target_arg, replacement):
    index = -1
    for i in range(0, len(runnable.program_args_list)):
        if runnable.program_args_list[i][1] == target_arg:
            index = i
            break
    if index == -1:
        return

    runnable.program_args_list[index][1] = replacement


def fill_my_endpoints(assigned):
    host: locals.HostNode = assigned[0]
    runnable: Runnable = assigned[1]

    port = locals.port_bases[runnable.component]
    my_endpoints_str = ""
    for i in range(0, runnable.num_cpus):
        endpoint = f"tcp+k2rpc://{host.fastip}:{port} "
        port += 1
        my_endpoints_str += endpoint

    replace_program_arg(runnable, "$my_endpoints", my_endpoints_str)


def fill_rdma(assigned):
    host: locals.HostNode = assigned[0]
    runnable: Runnable = assigned[1]
    replace_program_arg(runnable, "$rdma", host.rdma)


def fill_cpuset(assigned):
    host: locals.HostNode = assigned[0]
    runnable: Runnable = assigned[1]

    cpuset_str = ""
    for i in range(0, runnable.num_cpus):
        cpuset_str += f"{host.cores[i]},"
    cpuset_str = cpuset_str[:-1]

    replace_program_arg(runnable, "$cpus_expand", cpuset_str)


def get_component_endpoints(assignment, component):
    endpoints = ""
    for node in assignment:
        if node[1] is not None and node[1].component == component:
            for arg in node[1].program_args_list:
                if arg[0] == "tcp_endpoints":
                    endpoints += arg[1]
    return endpoints


def fill_cpo_endpoints(assignment, assigned):
    cpo_endpoints = get_component_endpoints(assignment, "cpo")
    cpo_endpoints = cpo_endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
    replace_program_arg(assigned[1], "$cpo_endpoints", cpo_endpoints)


def fill_tso_endpoints(assignment, assigned):
    tso_endpoints = get_component_endpoints(assignment, "tso")
    # TSO handles its own endpoints for workers, so we just need the master endpoint
    try:
        tso_endpoint = tso_endpoints.split()[0]
    except:
        return
    tso_endpoint = tso_endpoint.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
    replace_program_arg(assigned[1], "$tso_endpoints", tso_endpoint)


def fill_persist_endpoints(assignment, assigned):
    persist_endpoints = get_component_endpoints(assignment, "persist")
    persist_endpoints = persist_endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
    if persist_endpoints == "":
        return
    persist_endpoints_list = persist_endpoints.split()

    runnable: Runnable = assigned[1]
    shift = runnable.num_cpus

    if len(persist_endpoints_list) <= shift:
        replace_program_arg(assigned[1], "$persist_endpoints", persist_endpoints)
        return

    id = int(runnable.name[len(runnable.component):])
    for i in range(0, id):
        persist_endpoints_list = persist_endpoints_list[shift:] + persist_endpoints_list[:shift]
    persist_endpoints = ""
    for endpoint in persist_endpoints_list:
        persist_endpoints += endpoint + " "
    replace_program_arg(assigned[1], "$persist_endpoints", persist_endpoints)


def make_program_args(assigned):
    runnable: Runnable = assigned[1]

    for arg in runnable.program_args_list:
        runnable.program_args += f"--{arg[0]} {arg[1]} "


def check_assigned(assignment, component):
    for node in assignment:
        if node[1] is not None and node[1].component == component:
            return True

    return False


def add_runnable(assignment, runnable: Runnable):
    # First check for free assignment
    assigned = None
    host: locals.HostNode = None
    for node in assignment:
        print(len(node[0].cores))
        print(runnable.num_cpus)
        if node[0].config == runnable.target_config and node[1] is None and \
                runnable.num_cpus <= len(node[0].cores):
            assigned = node
            host = node[0]
            break
    if assigned is None:
        raise RuntimeError(f"No free hosts for {runnable}")

    # Second check for cluster prerequisites (e.g. CPO must be parsed before nodepool)
    can_assign = False
    # CPO, TSO, and persist are standalone with no prerequisites
    if runnable.component == "cpo" or runnable.component == "tso" or runnable.component == "persist":
        can_assign = True
    elif runnable.component == "nodepool":
        can_assign = check_assigned(assignment, "cpo") and check_assigned(assignment, "tso") and \
                     check_assigned(assignment, "persist")
    else:
        can_assign = check_assigned(assignment, "nodepool")
    if not can_assign:
        raise RuntimeError(f"Cluster prerequisites are not met for {runnable}")

    assigned[1] = runnable
    runnable.cpus = host.cores[:runnable.num_cpus]
    runnable.host = host.dns
    fill_my_endpoints(assigned)
    fill_rdma(assigned)
    fill_cpuset(assigned)
    fill_cpo_endpoints(assignment, assigned)
    fill_tso_endpoints(assignment, assigned)
    fill_persist_endpoints(assignment, assigned)
    # TODO
    make_program_args(assigned)


def remove_runnable(assignment, runnable: Runnable):
    index = -1
    for i in range(0, len(assignment)):
        if assignment[1].name == runnable.name:
            index = i
            break

    if index != -1:
        host = assignment[index][0]
        assignment[index] = [host, None]