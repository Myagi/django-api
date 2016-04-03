import copy

from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.core.exceptions import ValidationError

from rest_framework import serializers

import rest_pandas

from .pandas.io import read_frame

from .decorators import classproperty

from .utils.serializer_loading import get_serializer_for_model
from .fields import ExpandableRelatedField


RELATED_FIELD_SEPARATOR = "."
ALL_FIELDS_SPECIFIER = "*"
FIELDS_DIVIDER = ","


class ModelSerializer(serializers.HyperlinkedModelSerializer):

    """
    Base class from which all serializers should extend.
    Includes functionality which we want to share amongst all
    serializers (e.g. handling of the `fields` query parameter).
    """

    serializer_related_field = ExpandableRelatedField

    # Show id as well as URL for each
    # model by default
    id = serializers.IntegerField(read_only=True, source='pk')

    def _process_fields_based_on_context(self, all_fields):
        """
        Determines which fields on the serializer to include based on
        `self.context`, which may include a `requested_fields` value which
        indicates which fields should be included or a request which has
        a "fields" query that does the same thing.

        NOTE - This function mutates all_fields
        TODO - Decide whether to send BadRequest if invalid field specified
        """
        request = self.context.get("request")
        if request:
            request_params = getattr(request, "query_params", request.GET)
            fields_in_request = request_params.get("fields")
            if fields_in_request:
                fields_in_request = fields_in_request.split(FIELDS_DIVIDER)
        else:
            fields_in_request = None
        # requested_fields option takes precendant over fields requested query_params, as requested_fields option
        # is used by parent serializers to pass along relevant requested fields to nested serializers.
        fields = self.context.get("requested_fields", fields_in_request)
        if fields:
            relevant_fields = []
            for field in fields:
                try:
                    field = field.split(RELATED_FIELD_SEPARATOR)[0]
                    relevant_fields.append(field)
                except IndexError:
                    pass
            if not ALL_FIELDS_SPECIFIER in fields:
                # Drop any fields that are not specified in the `fields` argument.
                allowed = set(relevant_fields)
                existing = set(all_fields.keys())
                for field_name in existing - allowed:
                    all_fields.pop(field_name)
            # Get the relevant expansion information for related fields.
            fields_to_expand_map = {}
            for field in fields:
                try:
                    field_parts = field.split(RELATED_FIELD_SEPARATOR)
                    if len(field_parts) > 1:
                        key = field_parts[0]
                        if key not in fields_to_expand_map:
                            fields_to_expand_map[key] = []
                        fields_to_expand_map[key].append(RELATED_FIELD_SEPARATOR.join(field_parts[1:]))
                except IndexError:
                    pass
            for key, val in all_fields.iteritems():
                # Set requested_fields value for expandable fields.
                requested_fields = fields_to_expand_map.get(key, [])
                if hasattr(val, 'requested_fields'):
                    val.requested_fields = requested_fields
                elif hasattr(val, 'child_relation') and hasattr(val.child_relation, 'requested_fields'):
                    val.child_relation.requested_fields = requested_fields
        return all_fields

    def get_fields(self):
        """
        Limits the fields used during serialization based on value of `fields`
        query in request, or on the value of `requested_fields` which may be
        included in `self.context`.
        """
        fields = super(ModelSerializer, self).get_fields()
        fields = self._process_fields_based_on_context(fields)
        return fields

    def build_obj(self, data=None):
        """ Much of this is copied from the create method
        of ModelSerializer in DRF, as we need to be able
        to create an object using the serializer without saving it,
        which the create method does not allow.
        """
        from rest_framework.utils import model_meta
        from rest_framework.serializers import raise_errors_on_nested_writes

        # Copy as validated_data is mutated
        if data == None:
            data = self.validated_data
        validated_data = copy.copy(data)

        raise_errors_on_nested_writes('create', self, validated_data)

        ModelClass = self.Meta.model

        # Remove many-to-many relationships from validated_data.
        # They are not valid arguments to the default `.create()` method,
        # as they require that the instance has already been saved.
        info = model_meta.get_field_info(ModelClass)
        many_to_many = {}
        for field_name, relation_info in info.relations.items():
            if relation_info.to_many and (field_name in validated_data):
                many_to_many[field_name] = validated_data.pop(field_name)

        try:
            instance = ModelClass(**validated_data)
        except TypeError as exc:
            msg = (
                'Got a `TypeError` when calling `%s.objects.create()`. '
                'This may be because you have a writable field on the '
                'serializer class that is not a valid argument to '
                '`%s.objects.create()`. You may need to make the field '
                'read-only, or override the %s.create() method to handle '
                'this correctly.\nOriginal exception text was: %s.' %
                (
                    ModelClass.__name__,
                    ModelClass.__name__,
                    self.__class__.__name__,
                    exc
                )
            )
            raise TypeError(msg)

        return instance

    def call_model_clean_using_data(self, data):
        """ If validation logic is in model clean method,
        this can be used to trigger that method. """
        try:
            return self.build_obj(data).clean()
        except ValidationError as e:
            # Convert exception to DRF validation error
            # so that it is returned as a 400
            raise serializers.ValidationError(e.message)


class PolymorphicModelSerializer(ModelSerializer):

    """
    Can be used with polymorphic models to represent underlying
    subtypes for a model using the value of `subtype_serializers`,
    which is an attribute on the Meta class.
    """

    def to_representation(self, obj):
        """
        For a given model, attempts to find relevant serializer for that
        model using the serializers in `self.subtype_serializers`, then uses
        that serializer to represent the object.
        """
        subtype_serializers_map = {}
        for subtype_serializer in self.__class__.Meta.subtype_serializers:
            subtype_serializers_map[subtype_serializer.Meta.model] = subtype_serializer
        for model in subtype_serializers_map:
            if isinstance(obj, model):
                serializer = subtype_serializers_map[model]
                r = serializer(obj, context=self.context).to_representation(obj)
                return r
        # Could not find subtype serializer, so just just the serializer.
        return super(PolymorphicModelSerializer, self).to_representation(obj)


class PandasSerializer(ModelSerializer):

    @classmethod
    def get_dataframe_fieldnames(cls):
        return getattr(cls.Meta, 'fieldnames', [])

    @classmethod
    def transform_dataframe(cls, df):
        # Override if necessary
        return df


class PandasListSerializer(rest_pandas.PandasSerializer):
    """
    More performant version of original PandasSerializer from rest_pandas. Avoids
    proper serialization of each model in queryset and instead just uses read_frames
    func from django_pandas (which retrieves value lists from db as opposed to proper
    python objects).
    """
    @property
    def data(self):
        # Assumes self.instance is a queryset
        df = read_frame(
            self.instance,
            fieldnames=self.child.get_dataframe_fieldnames(),
            # No verbosity significantly reduces time it takes
            # to create dataframe
            verbose=False
        )
        df = self.child.transform_dataframe(df)
        return self.transform_dataframe(df)
