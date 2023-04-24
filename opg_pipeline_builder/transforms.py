import importlib
from pkgutil import iter_modules

from . import transform_engines


class Transforms:
    @staticmethod
    def _engine_name(base_name: str):
        split_name = base_name.split("_")
        proper = "".join([e[0].upper() + e[1:] for e in split_name])
        transform_name = proper + "TransformEngine"
        return transform_name

    def __init__(self):
        engines = [e.name for e in iter_modules(transform_engines.__path__)]
        engine_names = [self._engine_name(e) for e in engines]
        engine_zip = zip(engines, engine_names)
        for e, en in engine_zip:
            engine = importlib.import_module(
                f".{e}", package=transform_engines.__package__
            )
            engine_class = getattr(engine, en)
            setattr(self, e, engine_class)

    def __setattr__(self, key, value):
        base_engine = importlib.import_module(
            ".base", package=transform_engines.__package__
        )
        base_engine_class = getattr(base_engine, "BaseTransformEngine")

        if key in self.__dict__:
            try:
                engine = importlib.import_module(
                    f".{key}", package=transform_engines.__package__
                )
                engine_class = getattr(engine, self._engine_name(key))

            except ModuleNotFoundError:
                engine_class = base_engine_class

            if issubclass(value, engine_class):
                self.__dict__[key] = value
            else:
                raise TypeError(f"Value must be a {engine_class.__name__} object.")

        elif issubclass(value, base_engine_class):
            self.__dict__[key] = value

        else:
            raise TypeError("Value must be a child class of BaseTransformEngine.")
