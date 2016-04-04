import urlparse
import base64
import binascii
import imghdr
import uuid

from django.core.urlresolvers import resolve, Resolver404, get_script_prefix
from django.core.exceptions import ObjectDoesNotExist
from django.core.exceptions import ValidationError
from django.core.files.base import ContentFile
from django.utils import six
from django.utils.translation import ugettext_lazy as _
from django.core.validators import URLValidator

from rest_framework import serializers
from rest_framework import relations
from rest_framework.relations import PKOnlyObject
from rest_framework.fields import ImageField, FileField, SkipField
from rest_framework.compat import unicode_to_repr

from .utils import get_view_class

import re


ALLOWED_IMAGE_TYPES = (
    "jpeg",
    "jpg",
    "png",
    "gif"
)

EMPTY_VALUES = (None, '', [], (), {})

PDF_EXTENSION = 'pdf'


def _get_view_name_for_model_detail(model):
    """ Assumes that view name can be determined by lowercasing
    model name.
    TODO - Avoid making this assumption!
    """
    return "%s-detail" % model.__class__.__name__.lower()


class HyperlinkedPolymorphicRelatedField(serializers.HyperlinkedRelatedField):

    """ Represents related polymorphic model using specific URL for that model
    type, rather than a URL for the base model. Intended to be used in
    conjunction with views which use PolymorphicModelSerializer to
    represent multiple subtypes for entities.
    """

    def get_url(self, obj, view_name, request, format):
        """ Given an object, return the URL that hyperlinks to the object.
        May raise a `NoReverseMatch` if the `view_name` and `lookup_field`
        attributes are not configured to correctly match the URL conf.
        """

        view_name = _get_view_name_for_model_detail(obj)
        return super(HyperlinkedPolymorphicRelatedField, self).get_url(obj, view_name, request, format)


class ManyRelatedField(relations.ManyRelatedField):

    def __init__(self, *args, **kwargs):
        # This function can be used to filter related fields.
        self._filter_qs_func = kwargs.pop('filter_queryset')
        return super(ManyRelatedField, self).__init__(*args, **kwargs)

    def to_representation(self, iterable):
        # Ensure that filter_queryset function is applied to field value.
        if self._filter_qs_func:
            iterable = self._filter_qs_func(iterable)
        return [
            self.child_relation.to_representation(value)
            for value in iterable
        ]


class ExpandableRelatedField(serializers.HyperlinkedRelatedField):

    """ Related field which can expand if requested by client via query params.

    Also knows how to correctly represent related serializer if it is polymorphic.

    Also allows `filter_queryset` option to passed in, which is a function which can be
    used to filter the queryset on read (as opposed to on write, which is what the
    standard `queryset` option is for).
    """

    @classmethod
    def _apply_default_options(cls, kwargs):
        # Set default field style to input to prevent extremely slow
        # page loading times in the browsable API when there are many
        # choices for a field
        kwargs["style"] = kwargs.get("style", {"base_template": "input.html"})
        return kwargs

    def __new__(cls, *args, **kwargs):
        # Cannot just add this in init as it doesn't work when many=True
        kwargs = cls._apply_default_options(kwargs)
        return super(ExpandableRelatedField, cls).__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        kwargs = self._apply_default_options(kwargs)
        # filter_queryset is a function which can be passed in which
        # can restrict the queryset. Standard queryset argument is only
        # used on write.
        self._filter_qs_func = kwargs.pop('filter_queryset', None)
        # Allow for manual setting of _serializer_class.
        # If not provided, it will be inferred from view_name option
        self._serializer_class = kwargs.pop('serializer_class', None)
        # This value is set by ModelSerializer appropriately
        self.requested_fields = []
        super(ExpandableRelatedField, self).__init__(*args, **kwargs)

    @classmethod
    def many_init(cls, *args, **kwargs):
        """ """
        list_kwargs = {'child_relation': cls(*args, **kwargs)}
        for key in kwargs.keys():
            if key in relations.MANY_RELATION_KWARGS:
                list_kwargs[key] = kwargs[key]
        list_kwargs['filter_queryset'] = kwargs.get('filter_queryset')
        return ManyRelatedField(**list_kwargs)

    @property
    def serializer_class(self):
        """ Infers serializer class from view_name if `serializer_class` option
        is not provided on init.
        """
        if not getattr(self, '_serializer_class', None):
            view_class = get_view_class(self.view_name.replace('detail', 'list'))
            try:
                self._serializer_class = view_class.serializer_class
            except AttributeError:
                # This should never happen.
                raise Exception("The view class matching the specified view_name does not have an associated \
                    serializer_class and cannot be expanded")
        return self._serializer_class

    @property
    def serializer_instance(self):
        if not hasattr(self, '_serializer_instance'):
            self._serializer_instance = self.serializer_class(
                context={
                    "request": self.context["request"],
                    "requested_fields": self.requested_fields
                }
            )
        return self._serializer_instance

    def to_representation(self, value):
        """ If `self.requested_fields` has been set, then expand this field
        using related serializer. Otherwise, just return URL representation.
        """
        # if self._filter_qs_func:
        #     value = self._filter_qs_func(value)
        if self.requested_fields:
            r = self.serializer_instance.to_representation(value)
        else:
            r = super(ExpandableRelatedField, self).to_representation(value)
        return r

    def use_pk_only_optimization(self):
        """ If `self.requested_fields` has been set, then related resource will need to be
        expanded, therefore pk only optimization cannot be used.
        """
        if self.requested_fields:
            return False
        return super(ExpandableRelatedField, self).use_pk_only_optimization()

    @property
    def _serializer_is_polymorphic(self):
        from .serializers import PolymorphicModelSerializer
        return issubclass(self.serializer_class, PolymorphicModelSerializer)

    def get_url(self, obj, view_name, request, format):
        """ Given an object, return the URL that hyperlinks to the object.
        May raise a `NoReverseMatch` if the `view_name` and `lookup_field`
        attributes are not configured to correctly match the URL conf.

        If `self.serializer_class` is a PolymorphicModelSerializer, then
        view_name is inferred from obj type so that related models are correctly
        represented.
        """
        if self._serializer_is_polymorphic and not isinstance(obj, PKOnlyObject):
            view_name = _get_view_name_for_model_detail(obj)

        return super(ExpandableRelatedField, self).get_url(obj, view_name, request, format)

    def to_internal_value(self, data):
        """ TODO - I've had to copy this method from HyperlinkedRelatedField purely
        so that it can deal with polymorphic models. This probably is not ideal.
        """
        try:
            http_prefix = data.startswith(('http:', 'https:'))
        except AttributeError:
            self.fail('incorrect_type', data_type=type(data).__name__)

        if http_prefix:
            # If needed convert absolute URLs to relative path
            data = urlparse.urlparse(data).path
            prefix = get_script_prefix()
            if data.startswith(prefix):
                data = '/' + data[len(prefix):]

        try:
            match = resolve(data)
        except Resolver404:
            self.fail('no_match')

        if self._serializer_is_polymorphic:
            # TODO - This is not really what we want. Need to make sure
            # that match.view_name points to a view which uses a subtype
            # serializer for this polymorphic serializer
            self.view_name = match.view_name

        if match.view_name != self.view_name:
            self.fail('incorrect_match')

        try:
            return self.get_object(match.view_name, match.args, match.kwargs)
        except (ObjectDoesNotExist, TypeError, ValueError):
            self.fail('does_not_exist')


class Base64FileField(FileField):
    """ A django-rest-framework field for handling file-uploads through
    raw post data. It uses base64 for en-/decoding the contents of the file.

    Modified from https://github.com/Hipo/drf-extra-fields
    """

    def to_internal_value(self, base64_data):
        # Check if this is a base64 string
        if base64_data in EMPTY_VALUES:
            return None

        try:
            # Strip out the base64 prefix sent by browsers
            base64_data = re.sub(r"^data\:.+base64\,(.+)$", r"\1", base64_data)
        except TypeError:
            # This is already a file object (i.e. standard file
            # was uploaded in stead of base64 string).
            return super(Base64FileField, self).to_internal_value(base64_data)

        try:
            # If this is a valid URL, then assume that the file has not changed.
            # This was implemented to fix a prod only error, where patching a
            # PDF page without changing the pdf_file field caused an error
            # (as it was treated as a base64 string). Only happens in prod
            # because S3 is used there for media storage.
            val = URLValidator()
            val(base64_data)
            raise SkipField()
        except ValidationError:
            pass

        if isinstance(base64_data, six.string_types):
            # Try to decode the file. Return validation error if it fails.
            try:
                decoded_file = base64.b64decode(base64_data)
            except (TypeError, binascii.Error):
                raise ValidationError(_("Please upload a valid file."))
            # Generate file name.
            # 12 characters are more than enough.
            file_name = str(uuid.uuid4())[:12]
            # Get the file name extension.
            file_extension = self.get_file_extension(file_name, decoded_file)
            if not self.is_valid(file_name, decoded_file, file_extension):
                raise ValidationError(_("The type of the file could not be determined."))
            complete_file_name = file_name + "." + file_extension
            data = ContentFile(decoded_file, name=complete_file_name)
            return super(Base64FileField, self).to_internal_value(data)
        raise ValidationError(_('This is not a base64 string'))

    def is_valid(self, file_name, decoded_file, extension):
        return True

    def get_file_extension(self, file_name, decoded_file):
        return ''


def test_icc_profile_images(h, f):
    # imghdr is used by OrigBase64ImageField, however it does not work with jpegs that have
    # an ICC profile. adding this test fixes that.
    # See https://coderwall.com/p/btbwlq/fix-imghdr-what-being-unable-to-detect-jpegs-with-icc_profile
    if h.startswith('\xff\xd8') and h[6:17] == b'ICC_PROFILE':
        return "jpeg"


imghdr.tests.append(test_icc_profile_images)


class Base64ImageField(ImageField, Base64FileField):
    """ A django-rest-framework field for handling image-uploads through
    raw post data. It uses base64 for en-/decoding the contents of the file.

    Modified from https://github.com/Hipo/drf-extra-fields
    """

    def is_valid(self, file_name, decoded_file, extension):
        # Get the file name extension.
        file_extension = self.get_file_extension(file_name, decoded_file)
        return file_extension in ALLOWED_IMAGE_TYPES

    def get_file_extension(self, file_name, decoded_file):
        extension = imghdr.what(file_name, decoded_file)
        extension = "jpg" if extension == "jpeg" else extension
        return extension


class Base64PDFFileField(Base64FileField):
    """ TODO - Implement validation """

    def get_file_extension(self, file_name, decoded_file):
        return PDF_EXTENSION


class CurrentUserDefault(object):

    def set_context(self, serializer_field):
        self.user = serializer_field.context['request'].user

    def __call__(self):
        return self.user

    def __repr__(self):
        return unicode_to_repr('%s()' % self.__class__.__name__)


class CurrentUserDefaultOnCreate(object):

    def set_context(self, serializer_field):
        self.is_update = serializer_field.parent.instance is not None
        self.user = serializer_field.context['request'].user

    def __call__(self):
        if self.is_update:
            raise SkipField()
        return self.user

    def __repr__(self):
        return unicode_to_repr('%s()' % self.__class__.__name__)
