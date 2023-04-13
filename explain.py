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
conn.autocommit = True
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

    def __init__(self, explain_map):
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
        self.filter = explain_map.get("Filter")
        self.actual_startup_time = explain_map.get("Actual Startup Time")
        self.actual_total_time = explain_map.get("Actual Total Time")
        self.actual_rows = explain_map.get("Actual Rows")
        self.actual_loops = explain_map.get("Actual Loops")
        self.rows_removed_by_filter = explain_map.get("Rows Removed by Filter")
        self.hash_buckets = explain_map.get("Hash Buckets")
        self.workers = explain_map.get("Workers", [])

        self.children = [QueryNode(p) for p in explain_map.get("Plans", [])]

        self._explainMapping = {
            "Gather": self._explain_gather,
            "Hash Join": self._explain_hj,
            "Seq Scan": self._explain_ss,
            "Hash": self._explain_hash,
            "Merge Join": self._explain_merge_join,
            "Sort": self._explain_sort,
            "Nested Loop": self._explain_nl_join

        }

    # In natural language, explain what this node does.
    # We parse the explanation from bottom up.
    def explain(self) -> Tuple[List[Tuple[str, Dict[str, str]]], float, float]:
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
                        " more intermediate input, discussed immediately below.\n", None)
                )

        if self.node_type in self._explainMapping:
            res.append(self._explainMapping[self.node_type]())
        else:
            print(self.node_type + " is not supported")
            res.append(self._generic_explain())

        return res, self.total_cost, self.actual_total_time

    # explains itself only, does not parse the tree.
    def explain_self(self) -> Tuple[str, Dict[str, str]]:
        return self._explainMapping[self.node_type]()

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
            # TODO: If a high proportion of rows are being removed, you may want to investigate whether a (more) selective index could help.
            # Add a recommendation for the above, since we know the cardinality of each table, we can calculate the %
            # of filtered rows, and push an appropriate recommendation to build an index for this filter.
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
       return f"A sort operation is performed based on {self.sort_key} and is done in {self.sort_space_type}.", dict({
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
    
    def _explain_



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


# returns the query plan graph node
def get_query_plan(query: str) -> Tuple[List[Tuple[str, Dict[Any, Any]]], None] | Tuple[
    List[Tuple[str, Dict[str, str]]], QueryNode]:
    # we do not commit the transaction so analyze does not change db state
    cursor.execute("EXPLAIN (ANALYZE, COSTS, FORMAT JSON, VERBOSE, BUFFERS) " + query.rstrip(";") + ";")
    res = cursor.fetchone()
    if not res or not res[0]:
        print("no plan returned")
        return [("No plan returned", {})], None

    plan = res[0][0]["Plan"]
    sanitize_plan(plan)

    root_node = QueryNode(plan)
    res, _, _ = root_node.explain()
    for i, t in enumerate(res):
        s, info_d = t
        if i == len(res) - 1:
            res[i] = f"{i + 1}. Finally, {s[0].lower()}{s[1:]}", info_d
        else:
            res[i] = f"{i + 1}. {s}", info_d

    return res, root_node
    # print(res[0][0])


def sanitize_plan(plan):
    actual_ttl = plan["Actual Total Time"]
    for sub_plan in plan.get("Plans", []):
        if sub_plan["Actual Total Time"] > actual_ttl:
            sub_plan["Actual Total Time"] = actual_ttl - 0.01
            sub_plan["Actual Startup Time"] = plan["Actual Startup Time"] - 0.01
        sanitize_plan(sub_plan)
