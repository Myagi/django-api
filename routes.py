from rest_framework import routers

from .utils import get_classes_in_app_module

from .viewsets import BaseViewSet

router = routers.DefaultRouter()

def autodiscover_api_routes():
    """
    Looks for an api.views module in all INSTALLED_APPS.
    If module exists, adds all ModelViewSet classes in that module to
    a router and returns the resulting urls.
    """
    for viewset in get_classes_in_app_module('api.views', BaseViewSet):
        try:
            viewset.route_name
        except NotImplementedError:
            continue
        router.register(viewset.route_name, viewset, base_name=viewset.base_name)
    return router.urls
