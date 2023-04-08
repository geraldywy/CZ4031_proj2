# Here's some code anyone can copy and paste to reproduce your issue
import dearpygui.dearpygui as dpg

indent = 20

dpg.create_context()
dpg.create_viewport(title='INDENT TROUBLE', width=480, height=600)
dpg.setup_dearpygui()

with dpg.window(label="Main", width=440, height=500, tag='win'):
    with dpg.collapsing_header(label="Header 1", default_open=True):
        with dpg.tree_node(label="Tree node 1", default_open=True, indent=indent):
            str = "Number"
            dpg.add_input_int(label=str, width=240, tag=str)
        with dpg.tree_node(label="Tree node 2", default_open=True, indent=indent):
            str = "Numero"
            dpg.add_input_int(label=str, width=240, tag=str)
    with dpg.collapsing_header(label="Header 2", default_open=True):
        pass
dpg.show_viewport()
dpg.start_dearpygui()
dpg.destroy_context()
