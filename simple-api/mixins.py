"""
Miscellaneous mixins.
"""


from .decorators import classproperty 


class APIViewSetMixin(object):

    """
    Used to share common functionality
    between SwampDragon routers and DRF viewsets.
    """

    @classproperty
    @classmethod
    def serializer_class(cls):
        raise NotImplementedError('No serializer class for %s' % str(cls))

    @classproperty
    @classmethod
    def model(cls):
        return cls.serializer_class.Meta.model

    @classproperty
    @classmethod
    def queryset(cls):
        return cls.model.objects.all()

    @classproperty
    @classmethod
    def base_name(cls):
        """
        Used for view name (i.e. URL name).
        """
        return cls.model.__name__.lower()

    @classproperty
    @classmethod
    def route_name(cls):
        """
        Used for API route.
        """
        return '%ss' % cls.base_name
