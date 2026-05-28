__all__ = ["register_all", "seed_defaults"]


def __getattr__(name: str):
    if name in __all__:
        from . import bootstrap

        return getattr(bootstrap, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
