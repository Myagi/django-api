from rest_framework.permissions import BasePermission

from django.core.exceptions import ObjectDoesNotExist


class SimplePermission(BasePermission):

    """
    More functional than the basic DRF
    BasePermission class. Specify permissions for
    object detail views by overriding the can_* methods,
    or specify permissions for list views by overriding
    the get_* methods.

    Default is to use associated get_* method for all
    can_* methods.
    """

    @property
    def queryset(self):
        """
        Queryset should be set after init, just before permissions object is used
        to ensure filters have been applied.
        """
        if hasattr(self, "_queryset"):
            return self._queryset
        raise Exception("Queryset has not been set yet!")

    @queryset.setter
    def queryset(self, val):
        self._queryset = val

    def _use_queryset_method(self, method_name, user, obj):
        method = getattr(self, "get_" + method_name)
        qs = method(user)
        # If object is not in queryset which this user has permission
        # to view, then return False.
        return qs.filter(pk=obj.pk).first() != None

    def can_view(self, user, obj):
        return self._use_queryset_method("viewable", user, obj)

    def can_add(self, user, obj):
        raise NotImplementedError()

    def can_change(self, user, obj):
        return self._use_queryset_method("changeable", user, obj)

    def can_delete(self, user, obj):
        return self._use_queryset_method("deletable", user, obj)

    def get_viewable(self, user):
        raise NotImplementedError()

    def get_changeable(self, user):
        return self.get_viewable(user)

    def get_deletable(self, user):
        return self.queryset.none()

    def has_object_permission(self, request, view, obj):
        user = request.user
        self.queryset = view.get_queryset()
        if request.method == "GET":
            return self.can_view(user, obj)
        if request.method == "POST":
            return self.can_add(user, obj)
        if request.method in ["PUT", "PATCH"]:
            return self.can_change(user, obj)
        if request.method == "DELETE":
            return self.can_delete(user, obj)


class UseAssociatedObjectPerrmission(SimplePermission):

    """
    Allows permissions for one object to be determined by the
    permissions for an associated object.
    """

    @property
    def associated_object_permission(self):
        raise NotImplementedError()

    @property
    def associated_object_lookup(self):
        raise NotImplementedError()

    @property
    def associated_object_queryset(self):
        raise NotImplementedError()

    # During add/create, associated object may not be reachable
    # for a given instance. This variable determines whether that
    # is allowed or not.
    allow_broken_lookup_on_add = False

    def get_assoc_obj(self, obj):
        lookup = self.associated_object_lookup
        if not lookup:
            return obj
        parts = lookup.split('__')
        for part in parts:
            try:
                obj = getattr(obj, part)
            except ObjectDoesNotExist:
                return None
        return obj

    def _get_assoc_perms_instance(self):
        p = self.associated_object_permission()
        p.queryset = self.associated_object_queryset
        return p

    def _use_assoc_obj_perms(self, method_name, user, obj):
        perms_obj = self._get_assoc_perms_instance()
        assoc = self.get_assoc_obj(obj)
        if not assoc:
            # Use allow_broken_lookup_on_add field to determine what should
            # happen if a training plan cannot be reached on add.
            return self.allow_broken_lookup_on_add if method_name == "can_add" else False
        return getattr(perms_obj, method_name)(user, assoc)

    def can_add(self, user, obj):
        return self._use_assoc_obj_perms("can_add", user, obj)

    def can_change(self, user, obj):
        return self._use_assoc_obj_perms("can_change", user, obj)

    def can_view(self, user, obj):
        return self._use_assoc_obj_perms("can_view", user, obj)

    def can_delete(self, user, obj):
        return self._use_assoc_obj_perms("can_delete", user, obj)

    def _use_assoc_qs_perms(self, method_name, user):
        perms_obj = self._get_assoc_perms_instance()
        assoc_objs = getattr(perms_obj, method_name)(user)
        assoc_obj_ids = [assoc.id for assoc in assoc_objs]
        lookup = self.associated_object_lookup
        if not lookup:
            filter_kwargs = {
                "id__in": assoc_obj_ids
            }
        else:
            filter_kwargs = {
                lookup + "__id__in": assoc_obj_ids
            }
        return self.queryset.filter(**filter_kwargs)

    def get_viewable(self, user):
        return self._use_assoc_qs_perms("get_viewable", user)

    def get_changeable(self, user):
        return self._use_assoc_qs_perms("get_changeable", user)

    def get_deletable(self, user):
        return self._use_assoc_qs_perms("get_deletable", user)
