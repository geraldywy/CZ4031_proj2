import dearpygui.dearpygui as dpg

from explain import get_query_plan

old_query_ref: int | str = None
new_query_ref: int | str = None

old_g: int | str = None
new_g: int | str = None
view_graphic_ref: int | str = None


def view_graphic_callback(sender, app_data, user_data):
    old_qep, new_qep = user_data
    # place graphical visualization in a separate window pop up
    with dpg.window(label="query diff", width=700, height=600, pos=(320, 70)):
        dpg.add_text(str(old_qep) + "\n\n\n" + str(new_qep), wrap=550)


def button_callback():
    if old_query_ref is None or new_query_ref is None:
        return

    old_q = dpg.get_value(old_query_ref)
    new_q = dpg.get_value(new_query_ref)
    # TODO: enclose this in a try and except and display errors back if any.
    old_qep = get_query_plan(old_q)
    new_qep = get_query_plan(new_q)

    dpg.configure_item(view_graphic_ref, show=True)
    dpg.set_item_user_data(view_graphic_ref, (old_qep, new_qep))

    # place natural lang explanation in primary window (this will be scrollable)
    dpg.add_text(f"Old Query Plan Explanation: \n\n", wrap=500, parent=old_g, color=[255, 255, 0])
    for s, d in old_qep:
        dpg.add_text(s + "\n", wrap=500, parent=old_g)
        if not d:
            continue
        with dpg.collapsing_header(parent=old_g, label="Misc Info") as x:
            t = dpg.add_table(label="Misc", parent=x, width=500, header_row=False)
            dpg.add_table_column(parent=t)
            dpg.add_table_column(parent=t)
            for k, v in d.items():
                with dpg.table_row(parent=t):
                    dpg.add_text(k)
                    dpg.add_text(v)

    dpg.add_text(f"New Query Plan Explanation: \n\n", wrap=500, parent=new_g, color=[255, 255, 0])
    for s, d in new_qep:
        dpg.add_text(s + "\n", wrap=500, parent=new_g)
        if not d:
            continue

        with dpg.collapsing_header(parent=new_g, label="Misc Info") as x:
            t = dpg.add_table(label="Misc", parent=x, width=500, header_row=False)
            dpg.add_table_column(parent=t)
            dpg.add_table_column(parent=t)
            for k, v in d.items():
                with dpg.table_row(parent=t):
                    dpg.add_text(k)
                    dpg.add_text(v)


def start():
    dpg.create_context()
    dpg.create_viewport(title='Postgres SQL Query Plan Visualizer', width=1450)
    dpg.setup_dearpygui()

    with dpg.window(label="Example Window") as main_window:
        dpg.set_primary_window(main_window, True)

        dpg.add_spacer(height=5)
        with dpg.group(horizontal=True):
            dpg.add_spacer(width=510)
            dpg.add_text("Postgres SQL Query Execution Plan Diff Visualizer", bullet=True, color=[255, 255, 0])
        dpg.add_spacer(height=30)
        with dpg.group(horizontal=True, horizontal_spacing=100):
            dpg.add_spacer(width=50)
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
            dpg.add_spacer(width=600)
            dpg.add_button(label="Explain Query Plan Diff", callback=button_callback)

        dpg.add_spacer(height=50)
        with dpg.group(horizontal=True):
            dpg.add_spacer(width=580)
            global view_graphic_ref
            view_graphic_ref = dpg.add_button(
                label="View graphical visualization",
                callback=view_graphic_callback,
                show=False,
            )

        dpg.add_spacer(height=50)
        with dpg.group(horizontal=True, horizontal_spacing=100):
            dpg.add_spacer(width=50)
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
