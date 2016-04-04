""" 
Functions and classes which make it possible to dynamically
load serializers for models. 
"""


from myagi_django.utils import get_classes_in_module


class NoSerializerException(Exception):
    
    """ 
    Raised when serializer cannot be found for a model in a given location.
    """

    def __init__(self, loc, model):
        """ 
        Pass a `loc` in which `model` could not be found.
        """
        message = "Could not find serializer in %s for model %s" % (loc, model)
        super(NoSerializerException, self).__init__(message)

_imported_serializers = {}

def get_serializer_for_model(model):
    """ 
    Searches for a serializer which corresponds to a given `model`. 
    Assumes that serializers are in the api.serializers module of 
    the model's app. Will raise an exception of serializer cannot be
    found. 
    TODO - Allow location to be customized. 
    """
    from myagi_django.api.serializers import ModelSerializer 
    if _imported_serializers.get(model):
        return _imported_serializers[model]
    app_module =  model.__module__.split('.')
    del app_module[-1]
    serializer_mod = '.'.join(app_module) + '.api.serializers'
    for cls in get_classes_in_module(serializer_mod, ModelSerializer):
        meta = getattr(cls, 'Meta', None)
        if not meta:
            continue 
        serializer_model = getattr(cls.Meta, 'model', None)
        if serializer_model == model:
            _imported_serializers[model] = cls 
            return cls 
    raise NoSerializerException(serializer_mod, model)

