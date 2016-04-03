"""
Miscellaneous API utility functions.
"""


import inspect
import importlib
import traceback
import sys

from django.conf import settings

from django.core.urlresolvers import reverse, resolve


def auto_create(Model, **kwargs):
    """
    Auto creates an instance of `model` with given `kwargs` when
    executed. Can be used as the default parameter for a relational
    field to auto create realted models for a resource.
    """
    return lambda : Model.objects.create(**kwargs)


def get_view_class(view_name, kwargs={}):
    """
    Given a `view_name` string and `kwargs` for that view,
    returns the corresponding view class.
    """
    view_func = resolve(reverse(view_name, kwargs=kwargs)).func
    module = importlib.import_module(view_func.__module__)
    view = getattr(module, view_func.__name__)
    return view


_imported_mods = {}
def import_from_string(s):
    """ Given a string in the format <module>.<func/class>, imports
    and returns the func/class.
    """
    global _imported_mods
    if not _imported_mods.get(s):
        parts = s.split(".")
        mod = ".".join(parts[:-1])
        attr = parts[-1]
        imported_mod = importlib.import_module(mod)
        _imported_mods[s] = getattr(imported_mod, attr)
    return _imported_mods[s]


def _get_subclasses(mod, basecls):
    """ Yield the classes in module `mod` that inherit from `basecls`. """
    for name in dir(mod):
        o = getattr(mod, name)
        try:
            if (o != basecls) and issubclass(o, basecls):
                yield o
        except TypeError as e:
            pass


def get_classes_in_module(mod, basecls):
    """ Import module represented by `mod` string and yield
    all classes in that module which inherit from `basecls`.
    """
    try:
        imported_mod = importlib.import_module(mod)
    except ImportError as e:
        # Fairly hacky way to differentiate betweeen import
        # errors because mod_path does not exist in app (which we
        # want to ignore), and import errors within the module itself
        if mod.split('.')[-1] not in e.message:
            print
            print "Exception when importing module:", mod
            print "Traceback:"
            print
            tb = traceback.format_exc()
            print tb
            raise e
        return
    for cls in _get_subclasses(imported_mod, basecls):
        yield cls


def get_classes_in_app_module(rel_mod_path, basecls):
    """ For each app, import `rel_mod_path` and yield all
    classes in that module which inherit from `basecls`.
    """
    for app in settings.INSTALLED_APPS:
        mod_path = app + '.' + rel_mod_path
        for cls in get_classes_in_module(mod_path, basecls):
            yield cls


def get_pk_from_url(url):
    url = url.strip('/')
    parts = url.split('/')
    return parts[-1]
