import dearpygui.dearpygui as dpg

dpg.create_context()

with dpg.theme() as borderless_child_theme:
    with dpg.theme_component(dpg.mvChildWindow):
        dpg.add_theme_color(dpg.mvThemeCol_Border, [0, 0, 0, 0])

with dpg.window(label="Tutorial", width=800, height=300):
    with dpg.group(horizontal=True):
        with dpg.child_window(width=-200):
            dpg.bind_item_theme(dpg.last_item(), borderless_child_theme)
            with dpg.group():
                with dpg.collapsing_header(
                        label="Collapsing Header (expands beyond group)"):  # this spills over to next horiz group
                    dpg.add_text(default_value="Some text")

                with dpg.tree_node(label="Tree Node (expands beyond group)"):
                    dpg.add_text(default_value="Some text")

        with dpg.group():
            dpg.add_text(default_value="Next horiz group")
            dpg.add_text(default_value="Some text")
            dpg.add_text(default_value="Some text")
            dpg.add_text(default_value="Some text")

dpg.create_viewport()
dpg.setup_dearpygui()
dpg.show_viewport()
dpg.start_dearpygui()
dpg.destroy_context()
