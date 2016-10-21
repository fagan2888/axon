import fnmatch
import os
import re
from subprocess import Popen, PIPE


def find_requirements():
    """
    Find all the module calls imports in project. Either from 'import module_name' or
    :return:
    """

    matches = []
    modules = set()

    for root, dirnames, filenames in os.walk('event'):
        for filename in fnmatch.filter(filenames, '*.py'):
            matches.append(os.path.join(root, filename))
            # print matches[-1]

            with open(matches[-1], "r") as file_content:
                batch = file_content.read()

                # regex -- findall
                # from ([\w\.]*) import
                module_list = re.findall('from ([\w\.]*) import', batch)
                module_tuple = (x.split('.')[0] for x in module_list)
                if module_list: modules.update(set(module_tuple))

                # regex -- findall
                # import ([\w\.]*)
                module_list = re.findall('import ([\w\.]*)', batch)
                module_tuple = (x.split('.')[0] for x in module_list)
                if module_list: modules.update(set(module_tuple))

    return modules


def find_module_version():
    p = Popen(["conda", "list"], stdout=PIPE)
    output, err = p.communicate()
    matches = re.findall(r'\n([\w\-]*)\s*([\w\.]*)\s*', output)
    conda_list = {}
    if matches:
        for match in matches:
            conda_list[match[0]] = match[0]+'=='+match[1]

    return conda_list


def build_output(modules, conda_list):
    requirements_txt = []
    not_found = []
    for module in modules:
        a = conda_list.get(module)
        if a:
            requirements_txt.append(a)
        else:
            not_found.append(a)

    return '\n'.join(requirements_txt)


if __name__ == "__main__":
    modules = find_requirements()
    conda_list = find_module_version()
    requirements = build_output(modules, conda_list)
    print requirements