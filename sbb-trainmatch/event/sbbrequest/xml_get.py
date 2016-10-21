from sbb_api import fulltag


def get_xml_element(element, tree_path_full_itin, items):
    """
    finds all elements that satisfy the full 'tree_path_full' from starting position 'element'.
    (recursive dfs implementation)

    :param element:
    :param tree_path_full_itin:
    :param items:
    :return: list of all elements found at tree_path_full_itin
    """

    children = element.findall(tree_path_full_itin[0])
    if len(tree_path_full_itin) > 1:
        for child in children:
            get_xml_element(child, tree_path_full_itin[1:], items=items)
    else:
        if children:
            items.extend(children)
    return


def get_nodes(root, short_path):
    """
    get nodes at location short_path starting at root (root can be 'relative', e.g. that of a sub-tree)

    :param root:
    :param short_path:
    :return:
    """

    long_path = [fulltag(x) for x in short_path]
    nodes = []
    get_xml_element(root, long_path, nodes)
    return nodes


def remove_non_ascii(s): return "".join(i for i in s if ord(i) < 128)
# from:  http://stackoverflow.com/questions/1342000/
#        how-to-make-the-python-interpreter-correctly-handle-non-ascii-characters-in-stri


def get_node_text_value(root, short_path):
    """
    Find node at 'short_path' from 'root' and return text content.
    Purposefully breaks if more than 1 node found

    :param root:
    :param short_path:
    :return:
    """

    value_list = get_nodes(root, short_path)
    if value_list:
        [value] = value_list  # Breaks if more than one item returned
        return value.text
    else:
        # No item found
        return ''
