from typing import List, Dict, Tuple

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
    inner_unique: bool = None
    hash_cond: str = None

    relation_name: str = None
    schema: str = None
    alias: str = None

    filter: str = None

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
        self.inner_unique = explain_map.get("Inner Unique")
        self.hash_cond = explain_map.get("Hash Cond")
        self.relation_name = explain_map.get("Relation Name")
        self.schema = explain_map.get("Schema")
        self.alias = explain_map.get("Alias")
        self.filter = explain_map.get("Filter")

        self.children = [QueryNode(p) for p in explain_map.get("Plans", [])]

        self._explainMapping = {
            "Gather": self._explain_gather,
            "Hash Join": self._explain_hj,
            "Seq Scan": self._explain_ss,
            "Hash": self._explain_hash,
        }

    # In natural language, explain what this node does.
    # We parse the explanation from bottom up.
    def explain(self) -> List[Tuple[str, Dict[str, str]]]:
        res = []
        for i, child in enumerate(self.children):
            res.extend(child.explain())
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

        return res

    def _explain_gather(self) -> Tuple[str, Dict[str, str]]:
        return f"A Gather operation is performed, combining the output of child nodes," \
               " which are executed by parallel workers." \
               "\nGather does not make any guarantee about ordering, unlike Gather Merge, " \
               "which preserves sort order.\n", {"Test": ":123"}

    def _explain_hj(self) -> Tuple[str, Dict[str, str]]:
        return f"A hash join is performed. Hash join is an implementation of join in which one of the" \
               " collections of rows to be joined is hashed on the join keys using a separate 'Hash' node. " \
               "Postgres then iterates over the other collection of rows, for each one looking it up in the" \
               " hash table to see if there are any rows it should be joined to.\n", {}

    def _explain_ss(self) -> Tuple[str, Dict[str, str]]:
        return f"A sequential scan is performed on the {self.schema + '.' if self.schema else ''}{self.relation_name}" \
               " relation.\n", {
            "Description": "A Sequential Scan reads the rows from the table, in order.\nWhen reading from a table,"
                           " Seq Scans (unlike Index Scans) perform a single read operation (only the table is read).\n",
        }

    def _explain_hash(self) -> Tuple[str, Dict[str, str]]:
        return f"A hash is performed, hashing the query rows for use by its parent operation, " \
               "usually used to perform a JOIN.\n", {}

    def _generic_explain(self) -> Tuple[str, Dict[str, str]]:
        return f"A {self.node_type} operation is performed.\n", {}


# returns the query plan graph node
def get_query_plan(query: str) -> List[Tuple[str, Dict[str, str]]]:
    # we do not commit the transaction so analyze does not change db state
    cursor.execute("EXPLAIN (ANALYZE, COSTS, FORMAT JSON, VERBOSE, BUFFERS) " + query.rstrip(";") + ";")
    res = cursor.fetchone()
    if not res or not res[0]:
        print("no plan returned")
        return [("No plan returned", {})]

    res = QueryNode(res[0][0]["Plan"]).explain()
    for i, t in enumerate(res):
        s, info_d = t
        if i == len(res) - 1:
            res[i] = f"{i + 1}. Finally, {s[0].lower()}{s[1:]}", info_d
        else:
            res[i] = f"{i + 1}. {s}", info_d

    return res
    # print(res[0][0])
