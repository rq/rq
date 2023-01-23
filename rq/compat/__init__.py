import sys


def is_python_version(*versions):
    for version in versions:
        if (sys.version_info[0] == version[0] and sys.version_info >= version):
            return True
    return False



