from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response


NO_LIMIT = ['0', u'0', 0]


class HeaderLimitOffsetPagination(LimitOffsetPagination):

    headers = {}
    default_limit = 20
    max_limit = 99999

    def get_limit(self, request):
        if self.limit_query_param:
            limit = request.query_params.get(self.limit_query_param)
            # If '0' is passed, then return all entities
            if limit in NO_LIMIT:
                return self.max_limit
        return super(HeaderLimitOffsetPagination, self).get_limit(request)

    def get_paginated_response(self, data):
        return Response(data=data, headers=self.get_pagination_headers())

    def get_pagination_headers(self):
        """
        Returns the pagination headers for a given request and page.
        """
        links = []

        siblings = {
            'prev': self.get_previous_link(),
            'next': self.get_next_link()
        }

        for rel, page_url in siblings.items():
            links.append('<%s>; rel="%s"' % (page_url, rel))

        headers = {
            'X-Result-Count': self.count,
        }

        if links:
            headers['Link'] = ', '.join(links)

        return headers
