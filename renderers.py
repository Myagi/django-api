from rest_framework import status

from rest_pandas.renderers import PandasCSVRenderer as BaseCSVRenderer

from pandas import DataFrame


ERRONEOUS_STR_1 = 'None,,'
CORRECT_STR_1 = ',,'
ERRONEOUS_STR_2 = 'None,'
CORRECT_STR_2 = ','

class PandasCSVRenderer(BaseCSVRenderer):

    def render(self, *args, **kwargs):
        out = super(PandasCSVRenderer, self).render(*args, **kwargs)
        # Pandas outputs 'None' text at start of first
        # two rows when encoding is set for the to_csv method.
        # No idea why, however this quick fix prevents those values
        # from being displayed to users.
        out = out.replace(ERRONEOUS_STR_1, CORRECT_STR_1)
        out = out.replace(ERRONEOUS_STR_2, CORRECT_STR_2)
        return out
