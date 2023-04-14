# Here's some code anyone can copy and paste to reproduce your issue
import dearpygui.dearpygui as dpg

#### change node color theme
color = (37, 28, 138)
with dpg.theme() as theme_id:
    dpg.add_theme_color(dpg.mvNodeCol_TitleBar, color, category=dpg.mvThemeCat_Nodes)

with dpg.window(label="", width=600, height=500) as ne_id:
    with dpg.node_editor():
        with dpg.node(pos=[10, 10], label="Node1") as n_id:
            dpg.set_item_theme(n_id, theme_id)  #### not worki

dpg.start_dearpygui()
