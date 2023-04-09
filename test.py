import dearpygui.dearpygui as dpg

dpg.create_context()


# callback runs when user attempts to connect attributes
def link_callback(sender, app_data):
    # app_data -> (link_id1, link_id2)
    print(app_data, sender)
    # dpg.add_node_link(app_data[0], app_data[1], parent=sender)


# callback runs when user attempts to disconnect attributes
def delink_callback(sender, app_data):
    # app_data -> link_id
    dpg.delete_item(app_data)


with dpg.window(label="Tutorial", width=400, height=400):
    a1, a3 = None, None
    with dpg.node_editor(callback=link_callback, delink_callback=delink_callback) as f:
        print(f)
        with dpg.node(label="Node 1") as x:
            k = dpg.add_node_attribute(label="Node A1", parent=x)
            dpg.add_input_float(label="F1", width=150, parent=k)

            q = dpg.add_node_attribute(label="Node A1", parent=x, attribute_type=dpg.mvNode_Attr_Output)
            dpg.add_input_float(label="F2", width=150, parent=q)
            print("q", q)

        with dpg.node(label="Node 2") as x:
            k = dpg.add_node_attribute(label="Node A3", parent=x)
            dpg.add_input_float(label="F3", width=150, parent=k)

            h = dpg.add_node_attribute(label="Node A4", parent=x)
            dpg.add_input_float(label="F4", width=150, parent=h)
            print("to", h)

        dpg.add_node_link(q, h, parent=f)

dpg.create_viewport(title='Custom Title', width=800, height=600)
dpg.setup_dearpygui()
dpg.show_viewport()
dpg.start_dearpygui()
dpg.destroy_context()
