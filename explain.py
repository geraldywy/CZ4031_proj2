from collections import defaultdict, deque
from typing import List, Dict, Tuple, Any

import psycopg2

DATABASE = "TPC-H"
HOST = "localhost"
USER = "postgres"
PASSWORD = "password"
PORT = 5432

conn = psycopg2.connect(database=DATABASE,
                        host=HOST,
                        user=USER,
                        password=PASSWORD,
                        port=PORT)
conn.autocommit = False
cursor = conn.cursor()


class QueryNode:
    children: List = None
    node_type: str = None
    parallel_aware: str = None
    startup_cost: float = None
    total_cost: float = None
    plan_rows: int = None
    plan_width: int = None
    output: List[str] = None
    workers_planned: int = None
    single_copy: bool = None

    # extras for intermediate nodes
    parent_relationship: str = None
    scan_direction: str = None
    index_name: str = None
    join_type: str = None
    join_filter: str = None
    inner_unique: bool = None
    hash_cond: str = None

    relation_name: str = None
    schema: str = None
    alias: str = None

    sort_key: List = None
    sort_method: str = None
    sort_space_type: str = None
    merge_cond: str = None
    index_cond: str = None
    filter: str = None
    actual_startup_time = None
    actual_total_time = None
    actual_rows = None
    actual_loops = None
    hash_buckets = None
    workers: List[Dict[str, str]] = None

    # individual operation cost
    op_cost: float = None
    actual_op_cost: float = None
    plan_total_cost: float = None
    plan_total_time: float = None

    # plan-wide
    costliest_node = None
    slowest_node = None
    planning_time = None
    execution_time = None

    def __init__(self, explain_map, plan_total_cost=None, plan_total_time=None):
        self.node_type = explain_map.get("Node Type")
        self.parallel_aware = explain_map.get("Parallel Aware")
        self.startup_cost = explain_map.get("Startup Cost")
        self.total_cost = explain_map.get("Total Cost")
        self.plan_rows = explain_map.get("Plan Rows")
        self.plan_width = explain_map.get("Plan Width")
        self.output = explain_map.get("Output")
        self.workers_planned = explain_map.get("Workers Planned")
        self.single_copy = explain_map.get("Single Copy")
        self.parent_relationship = explain_map.get("Parent Relationship")
        self.scan_direction = explain_map.get("Scan Direction")
        self.index_name = explain_map.get("Index Name")
        self.join_type = explain_map.get("Join Type")
        self.join_filter = explain_map.get("Join Filter")
        self.inner_unique = explain_map.get("Inner Unique")
        self.hash_cond = explain_map.get("Hash Cond")
        self.relation_name = explain_map.get("Relation Name")
        self.schema = explain_map.get("Schema")
        self.alias = explain_map.get("Alias")
        self.sort_key = explain_map.get("Sort Key")
        self.sort_method = explain_map.get("Sort Method")
        self.sort_space_type = explain_map.get("Sort Space Type")
        self.merge_cond = explain_map.get("Merge Cond")
        self.index_cond = explain_map.get("Index Cond")
        self.filter = explain_map.get("Filter")
        self.actual_startup_time = explain_map.get("Actual Startup Time")
        self.actual_total_time = explain_map.get("Actual Total Time")
        self.actual_rows = explain_map.get("Actual Rows", 0)
        self.actual_loops = explain_map.get("Actual Loops")
        self.rows_removed_by_filter = explain_map.get("Rows Removed by Filter", 0)
        self.hash_buckets = explain_map.get("Hash Buckets")
        self.workers = explain_map.get("Workers", [])

        if not plan_total_cost and not plan_total_time:
            plan_total_cost = self.op_cost
            plan_total_time = self.actual_total_time
        self.plan_total_cost = plan_total_cost
        self.plan_total_time = plan_total_time

        self.children = [QueryNode(p, plan_total_cost, plan_total_time) for p in explain_map.get("Plans", [])]

        self._explainMapping = {
            "Gather": self._explain_gather,
            "Hash Join": self._explain_hj,
            "Seq Scan": self._explain_ss,
            "Hash": self._explain_hash,
            "Merge Join": self._explain_merge_join,
            "Sort": self._explain_sort,
            "Nested Loop": self._explain_nl_join,
            "Index Only Scan": self._explain_index_only_scan,
            "Index Scan": self._explain_index_scan
        }

    # In natural language, explain what this node does.
    # We parse the explanation from bottom up.
    def explain(self) -> Tuple[List[Tuple[str, Dict[str, str], Any]], float, float]:
        res = []
        self.op_cost = self.total_cost
        self.actual_op_cost = self.actual_total_time
        for i, child in enumerate(self.children):
            l, child_cost, actual_child_cost = child.explain()
            self.op_cost -= child_cost
            self.actual_op_cost -= actual_child_cost
            res.extend(l)
            if i + 1 < len(self.children):
                res.append(
                    (
                        f"The above output is then passed into a {self.node_type} operation as an input."
                        f" However, before we can process the {self.node_type} operation, "
                        f"we still have to process {len(self.children) - i - 1}"
                        " more intermediate input, discussed immediately below.\n", None, None)
                )

        if self.node_type not in self._explainMapping:
            print(self.node_type + " is not supported")
        res.append(self.explain_self())

        return res, self.total_cost, self.actual_total_time

    # explains itself only, does not parse the tree.
    def explain_self(self) -> Tuple[str, Dict[str, str], Any]:
        exp = self._explainMapping.get(self.node_type, self._generic_explain)()
        return exp[0], exp[1], self

    # analyze itself to get insights for the user
    # potential insights can include:
    # 1. Index scan more appropriate than a seq scan if filter
    #  condition removes large % of rows, recc to build an index
    # 2. Quality of estimation for plan rows vs actual rows
    # 3. % of time spent on this operation alone
    # 4. Whether this operation is slow. > 5ms (Slow), > 10ms (Very Slow)
    # 5. Estimated cost is high or not.
    # 6. If the sort is by a single column, or multiple columns from the same table,
    # you may be able to avoid it entirely by adding an index with the desired order.
    def get_node_insights(self) -> Dict[str, str]:
        if not self.actual_op_cost:
            self.explain()

        insights = {}  # label: Description

        # Checking potential scan optimisation
        if "scan" in self.node_type.lower() and self.filter:
            ttl_rows = self.actual_rows + self.rows_removed_by_filter
            perc_removed = self.rows_removed_by_filter / ttl_rows * 100
            if perc_removed > 70:
                if self.node_type == "Seq Scan":
                    insights[
                        "Filter Optimisation"] = f"{perc_removed:.2f}% of rows removed by filter condition.\n\n" \
                                                 f"Consider building an index on the filter condition as an " \
                                                 f"index scan might perform better."
                if self.node_type == "Index Scan":
                    insights[
                        "Filter Optimisation"] = f"{perc_removed:.2f}% of rows removed by filter condition.\n\n" \
                                                 f"Consider building indexes on the attributes in the filter " \
                                                 f"condition as an index only scan might perform better."

        # checking quality of row estimation
        est_rows_accuracy = abs(self.plan_rows - self.actual_rows) / max(1, self.actual_rows)
        if est_rows_accuracy < 0.5:
            insights["Row Estimation Quality"] = "Poor row estimation accuracy."
        elif est_rows_accuracy < 0.8:
            insights["Row Estimation Quality"] = "Decent row estimation accuracy."
        else:
            insights["Row Estimation Quality"] = "Good row estimation accuracy."

        # 3. % of time spent on operation
        insights["Percentage Of Time Spent On Operation"] = f"{self.actual_op_cost / self.plan_total_time * 100:.2f}%"

        # 4. whether this op is slow
        if 5 < self.actual_op_cost < 10:
            insights["Raw Speed"] = f"{self.actual_op_cost}ms.\n\nOperation is slow."
        elif self.actual_op_cost >= 10:
            insights["Raw Speed"] = f"{self.actual_op_cost}ms.\n\nOperation is very slow."

        # 5. estimated cost is high or low
        if 3000 < self.op_cost < 10000:
            insights["Estimated cost"] = f"{self.op_cost}.\n\nEstimated cost of operation is high."
        elif self.op_cost >= 10000:
            insights["Estimated cost"] = f"{self.op_cost}.\n\nEstimated cost of operation is very high."

        # 6. If the sort is by a single column, or multiple columns from the same table,
        # you may be able to avoid it entirely by adding an index with the desired order.
        tbl = None
        same_tbl = True
        if self.sort_key:
            for s in self.sort_key:
                tbl_name = s[:s.rfind(".")]
                if tbl is not None and tbl != tbl_name:
                    same_tbl = False
                    break
                tbl = tbl_name
            if self.node_type == "Sort" and len(self.sort_key) >= 1 and (len(self.sort_key) == 1 or same_tbl):
                insights[
                    "Potential sort index"] = "The sort is by a single column, or multiple columns from the same table.\n" \
                                              "You may be able to avoid it entirely by adding an index with the desired" \
                                              " order."
        return insights

    def _explain_gather(self) -> Tuple[str, Dict[str, str]]:
        return f"A Gather operation is performed on the output of {self.workers_planned} workers.", dict({
            "Description": "Gather combines the output of child nodes, which are executed "
                           "by parallel workers. Gather does not make any guarantee about "
                           "ordering, unlike Gather Merge, which preserves sort order.",
            **self._generic_explain_dict()
        })

    def _explain_hj(self) -> Tuple[str, Dict[str, str]]:
        return f"A hash join is performed on {self.hash_cond}.", dict({
            "Description": "Hash join is an implementation of join in which one of the"
                           " collections of rows to be joined is hashed on the join keys using a separate 'Hash' node. "
                           "Postgres then iterates over the other collection of rows, for each one looking it up in the"
                           " hash table to see if there are any rows it should be joined to.\n",
            "Join type": self.join_type
        }, **self._generic_explain_dict())

    def _explain_ss(self) -> Tuple[str, Dict[str, str]]:
        return f"A sequential scan is performed on the {self.schema + '.' if self.relation_name else ''}{self.relation_name}" \
               " relation.\n", dict({
            "Description": "A Sequential Scan reads the rows from the table, in order.\nWhen reading from a table,"
                           " Seq Scans (unlike Index Scans) perform a single read operation"
                           " (only the table is read).\n",
            "Relation": f"{self.schema + '.' if self.schema else ''}"
                        f"{self.relation_name}{f' as {self.alias}' if self.alias else ''}",
            "Filter condition": f"{self.filter}",
            "Rows removed by filter": f"{self.rows_removed_by_filter}\n\nThe per-loop average number of rows "
                                      f"removed by the filtering condition."
        }, **self._generic_explain_dict())

    def _explain_hash(self) -> Tuple[str, Dict[str, str]]:
        return f"A hash is performed on the results of the above operation.\n", dict({
            "Description": "Hash Node generates a hash table from the records in the input recordset. "
                           "Hash is used by Hash Join.",
            "Hash Buckets": f"{self.hash_buckets}\n\nHashed data is assigned to hash buckets. "
                            "Buckets are doubled until there are enough, so they are always a power of 2."
        }, **self._generic_explain_dict())

    def _explain_merge_join(self) -> Tuple[str, Dict[str, str]]:
        return f"A merge join operation is performed on {self.merge_cond}.", dict({
            "Description": "Merge Join is when two lists are sorted on their join keys before being joined together.\n"
                           "Postgres then traverse over the two lists in order, finding pairs that have identical join keys"
                           " and returning them as a new, joined row.\n",
            "Join type": self.join_type,
            "Parent Relationship": self.parent_relationship
        }, **self._generic_explain_dict())

    def _explain_sort(self) -> Tuple[str, Dict[str, str]]:
        return f"A sort operation is performed based on {','.join(self.sort_key)} and is done in {self.sort_space_type}.", dict(
            {
                "Description": "Sorting is performed as a result of an ORDER BY clause.\n"
                               "Sorting is expensive in terms of time and memory. The work_mem setting determines how much memory is given to Postgres per sort.\n"
                               "If sorting requires more memroy than work_mem, it will be carried out on the disk with slower speed.\n",
                "Join type": self.join_type,
                "Parent Relationship": self.parent_relationship,
                "Sort Method": self.sort_method.capitalize()
            }, **self._generic_explain_dict())
    
    def _explain_nl_join(self) -> Tuple[str, Dict[str, str]]:
       return f"A Nested Loop Join operation is performed on {self.join_filter}.", dict({
            "Description": "Nested Loop Join is run by iterating through one list, and for every row it contains, its corresponding"
                           "partner is looked up in the other list.\n"
                           "This is effective when one of the lists are very small, resulting in a small number of loops being run\n",
            "Join type": self.join_type
        }, **self._generic_explain_dict()) 

    def _explain_index_only_scan(self) -> Tuple[str, Dict[str, str]]:
        return f"An index-only scan can retrieve all the necessary data from an index without having to access the table, provided that the required information is available in the index.\n", dict({
            "Description": "If the query includes a condition that can be satisfied by the index alone, "
                            "and all the columns needed for the query are included in the index, the database engine can perform an index-only scan to retrieve the data directly from the index.\n"
                            "This makes it faster than index scan and its performance can be seen in large datasets.\n",
            "Relation": f"{self.schema + '.' if self.schema else ''}"
                        f"{self.relation_name}{f' as {self.alias}' if self.alias else ''}",
            "Filter condition": f"{self.filter}",
            "Index Condition": f"{self.index_cond}"
        }, **self._generic_explain_dict()) 
    
    def _explain_index_scan(self) -> Tuple[str, Dict[str, str]]:
        return f"An index scan requires the accessing of the all the columns of the index to see if it matches the condition\n", dict({
            "Description": "The process of an index scan involves searching the index for rows that meet a specific condition and then fetching those rows from the table.\n"
             "This method can be highly efficient if only a small portion of the rows are required and can also be useful for retrieving rows in a specific order.\n"
             "This two-step process of index scan therefore, makes it slower than sequential scan if all rows are needed and no particular order is required\n",
            "Scan Direction": f"{self.scan_direction}",
            "Index Name": f"{self.index_name}",
            "Index Cond": f"{self.index_cond}"
        }, **self._generic_explain_dict())  

    def _explain_nl_join(self) -> Tuple[str, Dict[str, str]]:
        return f"A Nested Loop Join operation is performed on {self.join_filter}.", dict({
            "Description": "Nested Loop Join is run by iterating through one list, and for every row it contains, its corresponding"
                           "partner is looked up in the other list.\n"
                           "This is effective when one of the lists are very small, resulting in a small number of loops being run\n",
            "Join type": self.join_type
        }, **self._generic_explain_dict())

    def _explain_index_only_scan(self) -> Tuple[str, Dict[str, str]]:
        return f"An index-only scan can retrieve all the necessary data from an index without having to access the table, provided that the required information is available in the index.\n", dict(
            {
                "Description": "If the query includes a condition that can be satisfied by the index alone, "
                               "and all the columns needed for the query are included in the index, the database engine can perform an index-only scan to retrieve the data directly from the index.\n"
                               "This makes it faster than index scan and its performance can be seen in large datasets.\n",
                "Relation": f"{self.schema + '.' if self.schema else ''}"
                            f"{self.relation_name}{f' as {self.alias}' if self.alias else ''}",
                "Filter condition": f"{self.filter}",
                "Index Condition": f"{self.index_cond}"
            }, **self._generic_explain_dict())

    def _explain_index_scan(self) -> Tuple[str, Dict[str, str]]:
        return f"An index scan requires the accessing of the all the columns of the index to see if it matches the condition\n", dict(
            {
                "Description": "The process of an index scan involves searching the index for rows that meet a specific condition and then fetching those rows from the table.\n"
                               "This method can be highly efficient if only a small portion of the rows are required and can also be useful for retrieving rows in a specific order.\n"
                               "This two-step process of index scan therefore, makes it slower than sequential scan if all rows are needed and no particular order is required\n",
                "Scan Direction": f"{self.scan_direction}",
                "Index Name": f"{self.index_name}",
                "Index Cond": f"{self.index_cond}"
            }, **self._generic_explain_dict())

    def _generic_explain(self) -> Tuple[str, Dict[str, str]]:
        return f"A {self.node_type} operation is performed.\n", self._generic_explain_dict()

    def _generic_explain_dict(self) -> Dict[str, str]:
        parallel_str = "Yes" if self.parallel_aware else "No"
        if len(self.workers):
            parallel_str += f" ({len(self.workers)} workers)"
        return {
            "Parallel": parallel_str,
            "Startup cost": f"{self.startup_cost}\n\nNote: This value is unit free. It is merely an estimate correlated "
                            "with the amount of time taken to return the first row.",
            "Total cost": f"{self.total_cost}\n\nNote: This value is unit free. It is merely an estimate correlated "
                          "with the amount of time taken to return all rows and for all its child.",
            "Operation cost": f"{self.op_cost:.2f}\n\nThe cost estimated for this operation only.",
            "Planned Rows": f"{self.plan_rows}\n\nNumber of rows estimated to be returned.",
            "Planned Width": f"{self.plan_width}\n\nAverage number of bytes estimated in a row returned "
                             f"by the operation.",
            "Actual startup time": f"{self.actual_startup_time}\n\nThe amount of time, in milliseconds, it takes to get "
                                   f"the first row out of the operation.",
            "Actual total time": f"{self.actual_total_time}\n\nThe actual amount of time in milliseconds spent on "
                                 f"this operation and all of its children. It is a per-loop average, "
                                 f"rounded to the nearest thousandth of a millisecond.",
            # Actual operation time with subplans and CTE is tricky to calculate.
            "Actual Operation time": f"{self.actual_op_cost:.2f}\n\nThe actual time taken in milliseconds for this operation only.",
            "Actual Rows": f"{self.actual_rows}\n\nThe average number of rows returned by the operation per loop,"
                           f" rounded to the nearest integer.",
            "Actual Loops": f"{self.actual_loops}\n\nThe number of times the operation is executed.",
        }

    def __str__(self):
        return self.node_type

    def get_plan_insight(self):
        if self.costliest_node is None or self.slowest_node is None:
            return {}

        return {
            "Slowest Operation": f"{self.slowest_node.node_type} took {self.slowest_node.actual_op_cost:.2f}ms.",
            "Costliest Operation": f"{self.costliest_node.node_type} was estimated at a cost of {self.costliest_node.op_cost:.2f}.",
            "Planning Time": f"{self.planning_time}ms",
            "Plan Execution Time": f"{self.execution_time}ms"
        }


# returns the query plan graph node
def get_query_plan(query: str, enable_hj: bool, enable_mj: bool, enable_nfl: bool, enable_ss: bool) -> Tuple[List[
    Tuple[str, Dict[Any, Any], Any]], None] | Tuple[List[Tuple[str, Dict[str, str], Any]], QueryNode]:
    # we do not commit the transaction so analyze does not change db state
    try:
        cursor.execute(f"set enable_hashjoin = {'true' if enable_hj else 'false'};")
        cursor.execute(f"set enable_mergejoin = {'true' if enable_mj else 'false'};")
        cursor.execute(f"set enable_nestloop = {'true' if enable_nfl else 'false'};")
        cursor.execute(f"set enable_seqscan = {'true' if enable_ss else 'false'};")
        cursor.execute("EXPLAIN (ANALYZE, COSTS, FORMAT JSON, VERBOSE, BUFFERS) " + query.rstrip(";") + ";")
        r = cursor.fetchone()
    except Exception as e:
        conn.rollback()
        raise e
    
    if not r or not r[0]:
        print("no plan returned")
        return [("No plan returned", {}, None)], None

    plan = r[0][0]["Plan"]
    sanitize_plan(plan)

    root_node = QueryNode(plan)
    res, _, _ = root_node.explain()
    # formatting and mark costliest and slowest node in plan
    costliest = None
    slowest = None
    for i, t in enumerate(res):
        s, info_d, node = t
        if i == len(res) - 1:
            res[i] = f"{i + 1}. Finally, {s[0].lower()}{s[1:]}", info_d, node
        else:
            res[i] = f"{i + 1}. {s}", info_d, node

        if node is None:
            continue
        if costliest is None or node.op_cost > costliest.op_cost:
            costliest = node
        if slowest is None or node.actual_op_cost > slowest.actual_op_cost:
            slowest = node

    root_node.slowest_node = slowest
    root_node.costliest_node = costliest
    slowest.slowest_node = slowest
    costliest.costliest_node = costliest

    root_node.planning_time = r[0][0].get("Planning Time", "NA")
    root_node.execution_time = r[0][0].get("Execution Time", "NA")

    return res, root_node


def sanitize_plan(plan):
    actual_ttl = plan["Actual Total Time"]
    for sub_plan in plan.get("Plans", []):
        if sub_plan["Actual Total Time"] > actual_ttl:
            sub_plan["Actual Total Time"] = actual_ttl - 0.01
            sub_plan["Actual Startup Time"] = plan["Actual Startup Time"] - 0.01
        sanitize_plan(sub_plan)


# Takes in 2 query node, returns a nested dict describing the diff
# the outer key is the category of node_type, with a corresponding list of dict describing
# each diff identified
def get_plan_diff(old_root, new_root) -> Dict[str, List[Dict[str, str]]]:
    res = defaultdict(list)

    old_scans = _identify_scans(old_root)
    new_scans = _identify_scans(new_root)
    for k, v in old_scans.items():
        if k in new_scans:
            tmp = {
                "Old scan time taken": f"{v.actual_op_cost:.2f}ms",
                "New scan time taken": f"{new_scans[k].actual_op_cost:.2f}ms",
                "Relation": k,
            }
            if v.node_type != new_scans[k].node_type:
                tmp[
                    "Description"] = f"In old plan, a {v.node_type} on {k} was done{f' with filter: {v.filter}' if v.filter else ''}. In the new plan, a {new_scans[k].node_type} on {k} was done{f' with filter: {new_scans[k].filter}' if new_scans[k].filter else ''} instead."
            else:
                filter_str = ""
                if v.filter or new_scans[k].filter:
                    filter_str = "\nBoth scans were also performed with the same filter condition."
                if v.filter != new_scans[k].filter:
                    filter_str = f"\nHowever, the old plan scan was performed with a filtering condition of {v.filter}, " \
                                 f"while the new plan scan was performed with a filtering condition of {new_scans[k].filter}"
                tmp["Description"] = f"In both plans, a {v.node_type} scan was performed on {k}." + filter_str
            res["Scans"].append(tmp)

    old_joins = _identify_joins(old_root)
    new_joins = _identify_joins(new_root)
    for k, v in old_joins.items():
        if k in new_joins:
            tmp = {
                "Old join time taken": f"{v.actual_op_cost:.2f}ms",
                "New join time taken": f"{new_joins[k].actual_op_cost:.2f}ms",
                "Join condition": k,
            }
            if v.node_type != new_joins[k].node_type:
                tmp[
                    "Description"] = f"In old plan, a {v.node_type} with join condition {k} was done. In the new plan, a {new_joins[k].node_type} with join condition {k} was done instead."
            else:
                tmp["Description"] = f"In both plans, a {v.node_type} was performed with join condition {k}."
            res["Joins"].append(tmp)

    return res


def _identify_scans(root: QueryNode) -> Dict[str, QueryNode]:
    queue = deque([root])

    scans = {}
    while queue:
        top = queue.popleft()
        if "scan" in top.node_type.lower() and top.relation_name:
            scans[top.relation_name] = top

        queue.extend(top.children)

    return scans


def _identify_joins(root: QueryNode) -> Dict[str, QueryNode]:
    queue = deque([root])

    joins = {}
    while queue:
        top = queue.popleft()
        join_cond = top.merge_cond or top.hash_cond
        if "join" in top.node_type.lower() and join_cond:
            joins[join_cond] = top

        queue.extend(top.children)

    return joins
