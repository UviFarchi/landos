"""
Platform package initializer.

When imported as ``backend.platform`` it exposes platform utilities.
When accidentally imported as top-level ``platform`` (e.g., stdlib lookup),
it defers to the stdlib platform module to avoid shadowing issues.
"""

if __name__ == "backend.platform":
    from backend.platform import utils  # re-export for convenience
    __all__ = ["utils"]
else:
    import importlib.util as _util
    import sysconfig as _sysconfig
    _path = _sysconfig.get_paths().get("stdlib")
    _spec = _util.spec_from_file_location("platform", f"{_path}/platform.py") if _path else None
    if _spec and _spec.loader:
        _module = _util.module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        globals().update(vars(_module))
    __all__ = []
