from rest_framework.validators import UniqueValidator


class CaseInsensitiveUniqueValidator(UniqueValidator):

    def filter_queryset(self, value, queryset):
        """
        Filter the queryset to all instances matching the given attribute.
        """
        filter_kwargs = {self.field_name + '__iexact': value}
        return queryset.filter(**filter_kwargs)
