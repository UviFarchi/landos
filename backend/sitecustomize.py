"""
Force stdlib 'platform' module to avoid shadowing by backend/platform package when
running with the backend directory on sys.path.
"""

import sys
import importlib.util
import sysconfig


def _load_stdlib_platform():
    stdlib_path = sysconfig.get_paths().get("stdlib")
    if not stdlib_path:
        return
    spec = importlib.util.spec_from_file_location("platform", f"{stdlib_path}/platform.py")
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules["platform"] = module


_load_stdlib_platform()
