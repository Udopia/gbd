# GBD Benchmark Database (GBD)
# Copyright (C) 2021 Jakob Bach and Markus Iser, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from gbd_tool.gbd_api import GBD
from gbd_tool.util import is_number

import mip
import pandas as pd

def optimal_comb(api: GBD, query, runtimes, timeout, k):
    result = api.query_search(query, [], runtimes)
    result = [[int(float(val)) if is_number(val) and float(val) < float(timeout) else int(2*timeout) for val in row[1:]] for row in result]
    dataset = pd.DataFrame(result, columns=runtimes)
    dataset = dataset[(dataset != 2*timeout).any(axis='columns')]
    model = mip.Model()
    instance_solver_vars = [[model.add_var(f'x_{i}_{j}', var_type=mip.BINARY)
                            for j in range(dataset.shape[1])] for i in range(dataset.shape[0])]
    solver_vars = [model.add_var(f's_{j}', var_type=mip.BINARY)for j in range(dataset.shape[1])]
    for var_list in instance_solver_vars:  # per-instance constraints
        model.add_constr(mip.xsum(var_list) == 1)
    for j in range(dataset.shape[1]):  # per-solver-constraints
        model.add_constr(mip.xsum(instance_solver_vars[i][j] for i in range(dataset.shape[0])) <=
                        dataset.shape[0] * solver_vars[j])  # "Implies" in Z3
    model.add_constr(mip.xsum(solver_vars) <= k)
    model.objective = mip.minimize(mip.xsum(instance_solver_vars[i][j] * int(dataset.iloc[i, j])
                                            for i in range(dataset.shape[0]) for j in range(dataset.shape[1])))
    print(dataset.sum().min())
    print(model.optimize())
    print(model.objective_value)
    for index, item in enumerate([var.x for var in solver_vars]):
        if item > 0:
            print(runtimes[index])

    