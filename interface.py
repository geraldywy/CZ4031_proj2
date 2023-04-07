import dearpygui.dearpygui as dpg
from explain import get_query_plan

old_query_ref: int | str = None
new_query_ref: int | str = None

display_text_ref: int | str = None

def button_callback():
    if old_query_ref is None or new_query_ref is None:
        return

    old_q = dpg.get_value(old_query_ref)
    new_q = dpg.get_value(new_query_ref)
    # TODO: enclose this in a try and except and display errors back if any.
    old_qep = get_query_plan(old_q)
    new_qep = get_query_plan(new_q)
    dpg.set_value(display_text_ref, str(old_qep) + "\n\n\n" + str(new_qep))


def start():
    dpg.create_context()
    dpg.create_viewport(title='Custom Title', width=1450)
    dpg.setup_dearpygui()

    with dpg.window(label="Example Window") as main_window:
        dpg.add_spacer(height=5)
        with dpg.group(horizontal=True):
            dpg.add_spacer(width=550)
            dpg.add_text("Postgres SQL Query Execution Plan Diff Visualizer")
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
                    default_value="SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey \nAND c.c_name LIKE '%cheng';",
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
            dpg.add_spacer(width=300)
            global display_text_ref
            display_text_ref = dpg.add_text(wrap=900)

        dpg.set_primary_window(main_window, True)  # Should fill viewport but does not on macOS

    dpg.show_viewport()
    dpg.start_dearpygui()

    dpg.destroy_context()
