from math import inf
from typing import Dict

import dearpygui.dearpygui as dpg

from explain import get_query_plan, QueryNode

old_query_ref: int | str = None
new_query_ref: int | str = None

old_g: int | str = None
new_g: int | str = None
labels: int | str = None
old_b: int | str = None
new_b: int | str = None


def view_graphic_callback(sender, app_data, user_data):
    root_node = user_data
    # place graphical visualization in a separate window pop up
    _build_graph_window(root_node)


def button_callback():
    if old_query_ref is None or new_query_ref is None:
        return

    old_q = dpg.get_value(old_query_ref)
    new_q = dpg.get_value(new_query_ref)
    # TODO: enclose this in a try and except and display errors back if any.
    old_qep, old_root_node = get_query_plan(old_q)
    new_qep, new_root_node = get_query_plan(new_q)

    dpg.show_item(labels)
    # place natural lang explanation in primary window (this will be scrollable)
    # first, clear prev items
    dpg.delete_item(old_g, children_only=True)
    dpg.delete_item(new_g, children_only=True)

    dpg.set_item_user_data(old_b, old_root_node)
    dpg.set_item_user_data(new_b, new_root_node)
    # dpg.add_spacer(width=50)

    for s, d in old_qep:
        dpg.add_text(s + "\n", wrap=500, parent=old_g)
        if not d:
            continue

        CollapsibleTable("Operation Details", "Operation Details", old_g, d, False)

    for s, d in new_qep:
        dpg.add_text(s + "\n", wrap=500, parent=new_g)
        if not d:
            continue
        CollapsibleTable("Operation Details", "Operation Details", new_g, d, False)


def start():
    dpg.create_context()

    dpg.create_viewport(title='Postgres SQL Query Plan Visualizer', width=1500)
    dpg.setup_dearpygui()

    with dpg.window(label="Example Window") as main_window:
        dpg.set_primary_window(main_window, True)

        dpg.add_spacer(height=5)
        with dpg.group(horizontal=True):
            dpg.add_spacer(width=510)
            dpg.add_text("Postgres SQL Query Execution Plan Diff Visualizer", bullet=True, color=[255, 255, 0])
        dpg.add_spacer(height=30)
        with dpg.group(horizontal=True, horizontal_spacing=100):
            dpg.add_spacer(width=100)
            with dpg.group():
                dpg.add_text("Old SQL Query")
                global old_query_ref
                old_query_ref = dpg.add_input_text(
                    default_value="SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey;",
                    multiline=True,
                    width=500,
                    height=300,
                    hint="Enter an SQL Query",
                )
            with dpg.group():
                dpg.add_text("New SQL Query")
                global new_query_ref
                new_query_ref = dpg.add_input_text(
                    default_value="SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey \n"
                                  "AND c.c_name LIKE '%cheng';",
                    multiline=True,
                    width=500,
                    height=300,
                    hint="Enter an SQL Query",
                )
            dpg.add_spacer(width=150)

        dpg.add_spacer(height=50)
        with dpg.group(horizontal=True):
            dpg.add_spacer(width=660)
            dpg.add_button(label="Explain Query Plan Diff", callback=button_callback)

        with dpg.group(horizontal=True, show=False) as la:
            global labels
            labels = la
            with dpg.group():
                dpg.add_spacer(height=50)
                with dpg.group(horizontal=True) as g:
                    dpg.add_spacer(width=130)
                    dpg.add_text(f"Old Query Plan Explanation: \n\n", wrap=500, color=[255, 255, 0], parent=g)
                    global old_b
                    old_b = dpg.add_button(
                        label="View old plan graphical visualization",
                        callback=view_graphic_callback,
                    )
                dpg.add_spacer(height=20)
            dpg.add_spacer(width=75)
            with dpg.group():
                dpg.add_spacer(height=50)
                with dpg.group(horizontal=True) as g:
                    dpg.add_spacer(width=130)
                    dpg.add_text(f"New Query Plan Explanation: \n\n", wrap=500, color=[255, 255, 0], parent=g)
                    global new_b
                    new_b = dpg.add_button(
                        label="View new plan graphical visualization",
                        callback=view_graphic_callback,
                    )
                dpg.add_spacer(height=20)

        with dpg.group(horizontal=True, horizontal_spacing=100):
            dpg.add_spacer(width=30)
            global old_g
            with dpg.group(label="Old Query Explanation", width=600) as g:
                old_g = g

            global new_g
            with dpg.group(label="New Query Explanation", width=600) as g:
                new_g = g

        dpg.add_spacer(height=100)

    dpg.show_viewport()
    dpg.start_dearpygui()

    dpg.destroy_context()


class CollapsibleTable:
    parent = None
    data = None
    active = None
    table_label = None
    t = None
    b = None
    button_label = None

    def __init__(self, button_label, table_label, parent, data, active=False):
        self.parent = parent
        self.data = data
        self.active = active
        self.button_label = button_label
        self.table_label = table_label

        self.b = dpg.add_button(parent=self.parent, label=f"{'V' if self.active else '>'} {self.button_label}",
                                callback=self._click)

        t = dpg.add_table(label=self.table_label, parent=self.parent, header_row=False, borders_innerV=True,
                          show=self.active, borders_innerH=True)
        dpg.add_table_column(parent=t)
        dpg.add_table_column(parent=t)
        for k, v in self.data.items():
            with dpg.table_row(parent=t) as r:
                dpg.add_text(k, parent=r)
                dpg.add_text(v, parent=r, wrap=280)
        dpg.add_spacer(parent=self.parent, height=30)
        self.t = t

    def _click(self):
        self.active = not self.active
        dpg.configure_item(self.t, show=self.active)
        dpg.configure_item(self.b, label=f"{'V' if self.active else '>'} {self.button_label}")


# offset from parent to child: (+300,+/-80)
def _build_graph_window(root_node: QueryNode):
    nodes_by_levels = [{None: [root_node]}]  # parent: child
    i = 0
    while i < len(nodes_by_levels):
        last_level = nodes_by_levels[i]
        nxt_level = {}
        for k, v in last_level.items():
            for n in v:
                t_n = []
                for child in n.children:
                    t_n.append(child)
                if t_n:
                    nxt_level[n] = sorted(t_n, key=lambda o: 0 if o.parent_relationship == "Outer" else 1)
        if nxt_level:
            nodes_by_levels.append(nxt_level)

        i += 1

    pos_map = {}
    x, y = 0, 0
    y_spacing = 250
    x_spacing = 250
    # initialization step, last column child, populate with coords first
    for k, l in nodes_by_levels[-1].items():
        for c in l:
            pos_map[c] = (x, y)
            y += y_spacing  # spacing between 2 nodes in same level
            # Note: We use the convention of outer relation on the top

    for i in range(len(nodes_by_levels) - 1, -1, -1):
        node_map = nodes_by_levels[i]
        for k, l in node_map.items():
            if k is None:
                break
            x -= x_spacing
            # ensure all nodes at this level have assigned pos
            # there can be a max of 2 children
            if len(l) > 2:
                print("More than 2 children graph not supported.")
                return
            min_y, max_y = inf, -inf
            for j, c in enumerate(l):
                if c not in pos_map:
                    if j + 1 < len(l):  # can safely assume there is an inner child then
                        pos_map[c] = (pos_map[l[j + 1]][0], pos_map[l[j + 1]][1] - y_spacing)
                    else:  # else, it is an inner child
                        pos_map[c] = (pos_map[l[j + 1]][0], pos_map[l[j + 1]][1] + y_spacing)

                min_y, max_y = min(pos_map[c][1], min_y), max(pos_map[c][1], max_y)
            if len(l) == 1:
                y_pos = min_y
            else:
                y_pos = int((min_y + max_y) / len(l))
            pos_map[k] = (x, y_pos)

    # apply appropriate offsets to make root_node be at pos(100,200)
    offset = (100 - pos_map[root_node][0], 200 - pos_map[root_node][1])
    for k in pos_map.keys():
        pos_map[k] = pos_map[k][0] + offset[0], pos_map[k][1] + offset[1]

    graph_ref = {}
    added_parent = False
    # place graphical visualization in a separate window pop up
    with dpg.window(label="Graph Viz", width=1250, height=600, pos=(150, 70)):
        with dpg.group():
            dpg.add_text("Click and drag nodes to rearrange them.")
            dpg.add_text("To reposition the camera, hold down left click and move mouse to corner of window.")
        with dpg.node_editor() as f:
            for lvl in nodes_by_levels[1:]:
                for p, l in lvl.items():
                    if p not in graph_ref:
                        graph_ref[p] = dpg.add_node(label=p.node_type, pos=pos_map[p], parent=f)

                    for c in l:
                        out = dpg.add_node_attribute(parent=graph_ref[p], attribute_type=dpg.mvNode_Attr_Output)
                        if not added_parent:
                            added_parent = True
                            s, d = p.explain_self()
                            dpg.add_text(summarise(s, d), parent=out, wrap=200)
                        graph_ref[c] = dpg.add_node(label=c.node_type, pos=pos_map[c], parent=f)
                        to = dpg.add_node_attribute(parent=graph_ref[c])

                        dpg.add_node_link(out, to, parent=f)
                        s, d = c.explain_self()
                        dpg.add_text(summarise(s, d), parent=to, wrap=200)


def summarise(s: str, d: Dict[str, str]) -> str:
    return d['Actual Operation time']
