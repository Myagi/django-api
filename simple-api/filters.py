from rest_framework.filters import DjangoFilterBackend  as DRFDjangoFilterBackend

from rest_framework_filters.filterset import FilterSet

from django_filters.filterset import FilterSetMetaclass

import rest_framework_filters as filters

from rest_framework.utils import model_meta


RELATED_FIELD_SEP = '__'


class NoValidationFilterSet(FilterSet):

    @property
    def qs(self):
        """ URGENT TODO - Figure out if there are any major security concerns with
        just blindly using filters like this. I've basically overriden the
        default implementation of this method from django-filters completely.
        The original does some checking to see if filters are valid based on it's
        internal form, which would always determine that related lookups like
        response__isnull were invalid (and would therefore return an empty qs).
        """
        filter_kwargs = {}
        for f in self.data:
            if f in self.filters:
                filter_kwargs[f] = self.data[f]
        return self.queryset.filter(**filter_kwargs)


class DjangoFilterBackend(DRFDjangoFilterBackend):

    """ Can be used to customize DRF filtering system. """

    # default_filter_set = NoValidationFilterSet

    def filter_queryset(self, request, queryset, view):
        """
        Can sometimes get duplicates in results, so have added distinct call to ensure
        these are removed.
        """
        qs = super(DjangoFilterBackend, self).filter_queryset(request, queryset, view)
        return qs.distinct()


_field_info_cache = {}


def _get_field_info(Model):
    if Model not in _field_info_cache:
        _field_info_cache[Model] = model_meta.get_field_info(Model)
    return _field_info_cache[Model]


def _get_related_model(Model, field_name):
    info = _get_field_info(Model)
    try:
        return info.relations[field_name].related_model
    except (AttributeError, KeyError):
        return None


def _get_model_field(Model, field_name):
    info = _get_field_info(Model)
    return info.fields_and_pk.get(field_name, info.relations.get(field_name))


def create_filter_class(Model, *args, **kwargs):

    """ Pass in a Model and a set of fields on that model. This
    function will return a FilterSet class which allows all lookup types
    (e.g. contains, exact, lt, gt ...) when filtering on the specified fields via the
    API.

    Also generates related filter sets. E.g. if somefield__first_name
    is specified, then all filter types will be available for somefield__first_name.

    Finally, if kwargs are interpreted as custom paramater to function mappings.
    If the parameter is included in the request, then the queryset will be passed into
    the associated function and a new queryset is assumed to be returned with some
    new query.
    """

    # Construct filter_set classes for any related fields
    related_fields = {}
    args = list(args)
    for f in args:
        if RELATED_FIELD_SEP in f or _get_related_model(Model, f):
            parts = f.split(RELATED_FIELD_SEP)
            fname = parts[0]
            if fname not in related_fields:
                related_fields[fname] = []
            if fname not in args:
                # Make sure the base field name is also in
                # args, or else filter class will not
                # work as expected.
                args.append(fname)
            rest = RELATED_FIELD_SEP.join(parts[1:])
            if rest:
                related_fields[fname].append(rest)
    related_classes = {}
    for f, model_fields in related_fields.iteritems():
        rel_model = _get_related_model(Model, f)
        # Recursively create related filter set
        related_classes[f] = create_filter_class(rel_model, *model_fields)

    def validate_field(f):
        if not _get_model_field(Model, f):
            raise Exception(
                "Field '%s' specified in create_filter_class func does not exist on %s" % (f, Model)
            )

    # Construct the class attributes for the new filter set.
    # Will included related fields and all lookups filter fields
    # where necessary.
    fields_on_model = []
    class_attrs = {}

    for f in args:
        # fields of related fields will be handled by the related_class
        # generated for that related field
        if RELATED_FIELD_SEP in f: continue
        validate_field(f)
        if f in related_classes:
            class_attrs[f] = filters.RelatedFilter(related_classes[f], name=f)
        else:
            class_attrs[f] = filters.AllLookupsFilter(name=f)
        fields_on_model.append(f)

    class AllLookupsFilterSetMetaclass(FilterSetMetaclass):

        def __new__(cls, name, bases, attrs):
            # On class instantiation, make sure generated fields are included
            attrs.update(class_attrs)
            return super(AllLookupsFilterSetMetaclass, cls).__new__(cls, name, bases, attrs)

    class AllLookupsFilterSet(FilterSet):

        __metaclass__ = AllLookupsFilterSetMetaclass

        class Meta:
            model = Model
            # NOTE - If this fields option is not specified,
            # then all model fields will be filterable
            fields = fields_on_model

        def __init__(self, *args, **kwargs):
            super(AllLookupsFilterSet, self).__init__(*args, **kwargs)

        @property
        def qs(self):
            qs = super(AllLookupsFilterSet, self).qs
            # kwargs can be used to provide extra functions which should be run
            # when particular params are included in the request
            for extra_filter in kwargs:
                if extra_filter in self.data:
                    qs = kwargs[extra_filter](qs, self.data[extra_filter])
            return qs

        def get_allowed_filter_names(self):
            return self.filters.keys() + kwargs.keys()

    # Initialising the filterset multiple times
    # prevents issues with related filters not working
    # until class has been used once. This is a bit of
    # hack...
    AllLookupsFilterSet()
    AllLookupsFilterSet()

    return AllLookupsFilterSet
