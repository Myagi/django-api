import traceback

from django.core.paginator import EmptyPage

import copy

import isodate

from rest_framework import filters
from rest_framework import viewsets
from rest_framework import serializers
from rest_framework.templatetags.rest_framework import replace_query_param
from rest_framework.response import Response
from rest_framework.utils import model_meta
from rest_framework.decorators import list_route

from .pagination import HeaderLimitOffsetPagination

import rest_pandas

import pandas as pd

import numpy as np

from .decorators import classproperty

from .mixins import APIViewSetMixin
from .serializers import RELATED_FIELD_SEPARATOR, ALL_FIELDS_SPECIFIER, FIELDS_DIVIDER, PandasListSerializer

from django_filters.filterset import filters_for_model, FilterSet

from .filters import DjangoFilterBackend

from .renderers import PandasCSVRenderer


FILTER_FIELDS_DEPTH = 2


DJANGO_RELATION_SEPARATOR = '__'


VALUES_OPT = 'values'
PIVOT_INDEX_OPT = 'indexes'
# Aggfuncs are available for timeseries
# manipulation as well
AGGFUNC_OPT = 'agg_funcs'
AGGFUNCS = ['sum', 'mean']
TIMESERIES_INDEX_OPT = 'index'
TIMESERIES_FREQ_OPT = 'freq'
TIMESERIES_GROUP_BY = 'group_by'
FILL_NA = 'fill_na'
CURRENT_METHOD = 'current_method'
TO_PIVOT = 'to_pivot_table'
TO_TIMESERIES = 'to_timeseries'
LIST_OPTS_DIVIDER = ','
RENAME_PREFIX = 'rename_'
DOWNLOAD_AS = 'download_as'
TIMESERIES_FILL_RANGE_START = 'fill_range_start'
TIMESERIES_FILL_RANGE_END = 'fill_range_end'
FILL_DATETIME_FORMAT = '%Y-%m-%d 00:00:00+00:00'

# NOTE - This override of the standard Pandas `sum` function
# is necessary to solve the segfaults which arise
# when the PandasViewSet transform_to_timeseries method runs.
# I'm not sure why this solves the problem, but it does. If you
# attempt to just use the standard np.sum function without wrapping
# it in another function, segfaults occur with some datasets (??!?).
# Speak to Alex if you have any related issues with this.
# NOTE - This function has to be named `sum` so that the dataframe
# column is also named `sum`.
_py_sum = sum


def sum(*args):
    return np.sum(*args)


def mean(x):
    # Ignore nan values when computing mean
    return np.nanmean(x)

AGGFUNC_OVERRIDES = {
    'sum': sum,
    'mean': mean,
    # Counts unique values
    'nunique': pd.Series.nunique
}

sum = _py_sum


def _get_filter_strings_for_model(model, depth, exclude=[]):
    info = model_meta.get_field_info(model)
    # Add base fields
    fields = info.fields.keys()
    if depth > 0:
        depth -= 1
        # Add related fields one level deep
        for field_name, rel in info.relations.iteritems():
            # Will recursively add related fields according to value of `depth`
            rel_field_names = [field_name + "__" + rel_field_name for rel_field_name in _get_filter_strings_for_model(rel.related_model, depth, exclude=exclude)]
            rel_field_names.append(field_name)
            fields += rel_field_names
    # filters_for_model is used by django_filters to determine
    # if fields we request to filter on are valid. So use that
    # to silently pass out any fields which django_filters will
    # complain about.
    all_filters = filters_for_model(
        model,
        fields=fields,
        exclude=exclude,
        filter_for_field=FilterSet.filter_for_field,
        filter_for_reverse_field=FilterSet.filter_for_reverse_field
    )
    tmp_fields = []
    for f in fields:
        if all_filters.get(f, False) == None:
            continue
        tmp_fields.append(f)
    fields = tmp_fields
    return fields


class BaseViewSet(APIViewSetMixin):

    """ Contains common functionality for all API viewsets, e.g.
    the ability to filter on all fields for a model by defaul.
    """

    pagination_class = HeaderLimitOffsetPagination
    filter_backends = (DjangoFilterBackend, filters.OrderingFilter,)
    # Override this for sensitive fields
    ordering_fields = '__all__'

    # This should not need to be changed.
    _filter_using_permissions = True

    def get_permissions(self):
        """ Adds support for the extra_permission_classes property
        which can add permissions on top of the default.
        """
        pc = super(BaseViewSet, self).get_permissions()
        if hasattr(self, "extra_permission_classes"):
            pc += [p() for p in self.extra_permission_classes]
        return pc

    def get_queryset(self):
        """ Adds support for filtering queryset based on "get_changeable",
        "get_updateable" and "get_deletable" methods which can exist on
        permissions classes.
        """
        user = self.request.user

        if self.request.method == "GET":
            pc_method = "get_viewable"
        elif self.request.method in ["PUT", "PATCH"]:
            pc_method = "get_changeable"
        elif self.request.method == "DELETE":
            pc_method = "get_deletable"
        else:
            pc_method = None

        qs = super(BaseViewSet, self).get_queryset()

        # Calculate `prefetch_related_args` based on the `fields` query param.
        # This optimises database fetch for data.
        prefetch_related_args = self.get_prefetch_related_args()
        if prefetch_related_args:
            qs = qs.prefetch_related(*prefetch_related_args)

        if pc_method and self._filter_using_permissions:
            for pc in self.get_permissions():
                if hasattr(pc, pc_method):
                    pc.queryset = qs
                    qs = getattr(pc, pc_method)(user)

        return qs

    def get_requested_fields(self):
        requested_fields = self.request.query_params.get("fields")
        if (requested_fields):
            requested_fields = requested_fields.split(FIELDS_DIVIDER)
        else:
            requested_fields = []
        return requested_fields

    def get_prefetch_related_args(self):
        """ Calculate prefetch_related args based on the `fields` query param.

        TODO - Need to validated requested fields!
        """
        requested_fields = self.get_requested_fields()
        relational_fields = []
        if (requested_fields):
            for field in requested_fields:
                if RELATED_FIELD_SEPARATOR in field:
                    split_field = field.split(RELATED_FIELD_SEPARATOR)
                    # All components except last are relational fields
                    relation_specifier = DJANGO_RELATION_SEPARATOR.join(split_field[:-1])
                    relational_fields.append(relation_specifier)
        extra_prefetch = self._get_prefetch_args_from_mapping(requested_fields)
        return list(set(relational_fields + extra_prefetch))

    def _get_prefetch_args_from_mapping(self, requested_fields):
        def _add(args, vals):
            if isinstance(vals, list):
                return args + vals
            return args.append(vals)
        mapping = self.get_fields_to_prefetch_mapping()
        prefetch_args = []
        for field in mapping:
            if ALL_FIELDS_SPECIFIER in requested_fields or field in requested_fields:
                _add(prefetch_args, mapping[field])
        if ALL_FIELDS_SPECIFIER in mapping and ALL_FIELDS_SPECIFIER in requested_fields:
            _add(prefetch_args, mapping[ALL_FIELDS_SPECIFIER])
        return prefetch_args

    def get_fields_to_prefetch_mapping(self):
        """ Override to add custom prefetch arguments on queryset when particular fields are requested. """
        return {}

    def get_object(self):
        # Disable filtering using permissions class, as we want individual object permissions
        # to be used instead of queryset level permissions. Object level permissions are
        # applied by super.get_object().
        self._filter_using_permissions = False
        obj = super(BaseViewSet, self).get_object()
        self._filter_using_permissions = True
        return obj

    def _build_obj_using_serializer(self, serializer):
        return serializer.build_obj()

    def perform_create(self, serializer):
        """ Ensures that object permissions are checked even for POST requests. """
        obj = self._build_obj_using_serializer(serializer)
        self.check_object_permissions(self.request, obj)
        return super(BaseViewSet, self).perform_create(serializer)


class ModelViewSet(BaseViewSet, viewsets.ModelViewSet):
    pass


class PandasViewSet(BaseViewSet, rest_pandas.PandasViewSet):

    pandas_serializer_class = PandasListSerializer
    renderer_classes = (PandasCSVRenderer,)

    def list(self, request, *args, **kwargs):
        # Same as original list method, but bypasses pagination
        # and passes queryset directly to serializer (whereas pagination converts queryset
        # to a list)

        queryset = self.filter_queryset(self.get_queryset())


        serializer = self.get_serializer(queryset, many=True)
        data = serializer.data
        r = Response(data)

        download_as = self.request.query_params.get(DOWNLOAD_AS)
        if download_as:
            # download_as param can be used to request a content disposition
            # of type attachment for the response. Allows CSVs to be downloaded.
            r['Content-Disposition'] = 'attachment; filename="%s"' % download_as
        return r

    def _get_aggfuncs(self, default=None):
        aggfuncs = self.request.query_params.get(AGGFUNC_OPT) or default
        if aggfuncs:
            aggfuncs = aggfuncs.split(LIST_OPTS_DIVIDER)
            override_aggfuncs = [AGGFUNC_OVERRIDES[aggfunc] for aggfunc in aggfuncs if aggfunc in AGGFUNC_OVERRIDES]
            np_aggfuncs = [getattr(np, aggfunc) for aggfunc in aggfuncs if aggfunc in AGGFUNCS and aggfunc not in AGGFUNC_OVERRIDES]
            aggfuncs = np_aggfuncs + override_aggfuncs
        return aggfuncs

    def _get_only_valid_func(self, df):
        return lambda i: i in df.columns

    def transform_to_pivot_table(self, dataframe):
        all_col_keys = [
            PIVOT_INDEX_OPT,
            VALUES_OPT
        ]
        dataframe = self._drop_columns_not_requested(dataframe, all_col_keys)
        pivot_values = self.request.query_params.get(VALUES_OPT)
        pivot_indexes = self.request.query_params.get(PIVOT_INDEX_OPT)
        # IF NOT ENOUGH ARGS, RAISE BAD REQUEST
        fieldnames = self.serializer_class.Meta.fieldnames
        only_valid = self._get_only_valid_func(dataframe)

        pivot_values = filter(only_valid, pivot_values.split(LIST_OPTS_DIVIDER))
        pivot_indexes = filter(only_valid, pivot_indexes.split(LIST_OPTS_DIVIDER))
        pt_kwargs = {
            'values': pivot_values,
            'index': pivot_indexes
        }
        aggfuncs = self._get_aggfuncs()
        if aggfuncs:
            pt_kwargs['aggfunc'] = aggfuncs
        dataframe = dataframe.pivot_table(**pt_kwargs)
        return dataframe

    def _attach_empty_row(self, dataframe, index_name, fill_range_start):
        empty_row = []
        for c in dataframe.columns:
            if c == index_name:
                empty_row.append(isodate.parse_datetime(fill_range_start))
            else:
                empty_row.append(0)
        dataframe.loc[0] = empty_row
        return dataframe

    def transform_to_timeseries(self, dataframe):
        # NOTE: Timeseries_index must be some date time field.
        # Most of the complexity here is required for allowing
        # multi-indexing (i.e. indexing on more than just the time value).
        # See http://stackoverflow.com/questions/15799162/resampling-within-a-pandas-multiindex.
        # Have used this http://stackoverflow.com/questions/18677271/grouping-daily-data-by-month-in-python-pandas-and-then-normalising
        # to simplify the code from the first answer.
        # Without multindexing, creation of timeseries dataframe is fairly easy (just set index
        # to a datetime column and then use resample method).
        all_col_keys = [
            TIMESERIES_INDEX_OPT,
            TIMESERIES_GROUP_BY,
            VALUES_OPT
        ]
        dataframe = self._drop_columns_not_requested(dataframe, all_col_keys)
        timeseries_index = self.request.query_params.get(TIMESERIES_INDEX_OPT)
        timeseries_freq = self.request.query_params.get(TIMESERIES_FREQ_OPT)
        timeseries_group_by = self.request.query_params.get(TIMESERIES_GROUP_BY)
        fill_range_start = self.request.query_params.get(TIMESERIES_FILL_RANGE_START)
        fill_range_end = self.request.query_params.get(TIMESERIES_FILL_RANGE_END)
        fieldnames = self.serializer_class.Meta.fieldnames
        only_valid = self._get_only_valid_func(dataframe)

        was_empty = False
        if dataframe.empty and fill_range_start and fill_range_end:
            # Create a fake row...this means that returned dataframe will be in same format
            # whether there is data or not
            dataframe = self._attach_empty_row(dataframe, timeseries_index, fill_range_start)
            was_empty = True

        dataframe = dataframe.set_index(timeseries_index)

        if timeseries_group_by:
            timeseries_group_by =  filter(only_valid, timeseries_group_by.split(LIST_OPTS_DIVIDER))
        if timeseries_freq:
            aggfuncs = self._get_aggfuncs()
            if timeseries_group_by:
                dataframe = dataframe.groupby(timeseries_group_by)
            # WARNING - This resampling has been known to cause segfaults with
            # numpy when the np.sum function is used. Not sure why, but possibly related
            # to https://github.com/numpy/numpy/issues/2995. Likely caused
            # by the above code to make resampling with multindexing a possibility
            # (i.e. resampling is not an issue when timeseries_group_by is not
            # used). See `AGGFUNC_OVERRIDES` variable above for explanation of
            # how this bug was fixed.
            dataframe = dataframe.resample(timeseries_freq, how=aggfuncs)
            # Fill empty date rows with zeros, rather than omitting them.
            if fill_range_start and fill_range_end:
                fill_range_start = isodate.parse_datetime(fill_range_start).strftime(FILL_DATETIME_FORMAT)
                fill_range_end = isodate.parse_datetime(fill_range_end).strftime(FILL_DATETIME_FORMAT)
                idx = pd.date_range(fill_range_start, fill_range_end)
                idx.name = timeseries_index
                dataframe = dataframe.reindex(idx, fill_value=0)
                if was_empty:
                    # Zero out all the rows with a 1...this should only happen when nunique
                    # aggfunc is used
                    dataframe = dataframe.replace(to_replace=1, regex=False, value=0)
        return dataframe

    def _get_cols_from_keys(self, dataframe, keys):
        only_valid = self._get_only_valid_func(dataframe)
        values = []
        for k in keys:
            v = self.request.query_params.get(k)
            if v:
                v = filter(only_valid, v.split(LIST_OPTS_DIVIDER))
                values += v
        return values

    def _drop_columns_not_requested(self, dataframe, requested_column_keys):
        """ Reduces the size of and computational cost of dataframe by
        removing columns which were not requested """
        values = self._get_cols_from_keys(dataframe, requested_column_keys)
        if values:
            to_drop = []
            for c in dataframe.columns:
                if c not in values:
                    to_drop.append(c)
            if to_drop:
                dataframe.drop(to_drop, axis=1, inplace=True)
        return dataframe

    def _rename_according_to_query(self, df):
        rename_map = {}
        for q in self.request.query_params:
            if q.startswith(RENAME_PREFIX):
                orig_name = q.replace(RENAME_PREFIX, '')
                rename_map[orig_name] = self.request.query_params[q]

        # Rename columns
        col_renames = {}
        for col in df.columns:
            if not isinstance(col, tuple):
                col = (col,)
            for name in col:
                if name in rename_map:
                    col_renames[name] = rename_map[name]
        df.rename(columns=col_renames, inplace=True)

        # Rename indexes
        new_index_names = []
        for index in df.index.names:
            if index in rename_map:
                new_index_names.append(rename_map[index])
            else:
                new_index_names.append(index)
        df.index.names = new_index_names

        return df

    def transform_dataframe(self, dataframe):
        current_method = getattr(self, CURRENT_METHOD, None)
        if current_method == TO_PIVOT:
            dataframe = self.transform_to_pivot_table(dataframe)
        elif current_method == TO_TIMESERIES:
            dataframe = self.transform_to_timeseries(dataframe)
        if not dataframe.empty:
            dataframe = self._rename_according_to_query(dataframe)
        # `fill_na` param has no effect on the actual calculation of data.
        # It just changes how NaN values appear when they are returned. It
        # is more about user experience than anything (i.e. what will look best
        # for end users). NOTE - Do not move the application of the `fill_na` value
        # so that it has an effect on calculations. This causes hard to find errors
        # during the transformation of dataframes.
        fill_na = self.request.query_params.get(FILL_NA)
        if fill_na != None: dataframe = dataframe.fillna(fill_na)
        return dataframe

    @list_route()
    def to_pivot_table(self, request):
        setattr(self, CURRENT_METHOD, TO_PIVOT)
        r = self.list(request)
        setattr(self, CURRENT_METHOD, None)
        return r

    @list_route()
    def to_timeseries(self, request):
        setattr(self, CURRENT_METHOD, TO_TIMESERIES)
        r = self.list(request)
        setattr(self, CURRENT_METHOD, None)
        return r

class ReadOnlyModelViewSet(BaseViewSet, viewsets.ReadOnlyModelViewSet):
    pass


class PolymorphicModelViewSet(BaseViewSet, viewsets.ReadOnlyModelViewSet):
    """ By default, polymorphic model view sets are read only """
    pass
