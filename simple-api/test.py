import urllib

from django.core.urlresolvers import reverse
from django.contrib.auth import get_user_model
from django.conf import settings
from django.db import transaction

from rest_framework.test import APITestCase as DRFAPITestCase, APIRequestFactory
from rest_framework import status

from .decorators import classproperty
from .utils import import_from_string


class _APITestCaseBase(DRFAPITestCase):

    """
    Defines convenient assertion methods and some other
    bits and pieces.
    """

    def reverse(self, *args, **kwargs):
        """
        Convenience method to avoid having to import reverse
        """
        return reverse(*args, **kwargs)

    def _assertStatus(self, response, status):
        try:
            self.assertEqual(response.status_code, status)
        except AssertionError as e:
            print
            print
            print "Status code assertion failed, response was:"
            print str(response)
            print
            raise e
        return response

    def assertOK(self, response):
        return self._assertStatus(response, status.HTTP_200_OK)

    def assertCreated(self, response):
        return self._assertStatus(response, status.HTTP_201_CREATED)

    def assertAccepted(self, response):
        return self._assertStatus(response, status.HTTP_202_ACCEPTED)

    def assertNoContent(self, response):
        return self._assertStatus(response, status.HTTP_204_NO_CONTENT)

    def assertForbidden(self, response):
        return self._assertStatus(response, status.HTTP_403_FORBIDDEN)

    def assertUnauthorized(self, response):
        print dir(status)
        return self._assertStatus(response, status.HTTP_401_UNAUTHORIZED)

    def assertMethodNotAllowed(self, response):
        return self._assertStatus(response, status.HTTP_405_METHOD_NOT_ALLOWED)

    def assertBadRequest(self, response):
        return self._assertStatus(response, status.HTTP_400_BAD_REQUEST)


class APIViewSetTestCase(_APITestCaseBase):

    """
    Subclass this when writing tests for an individual API view (or viewset).
    Shouldn't be used for integration tests.
    """

    @classproperty
    @classmethod
    def viewset(cls):
        raise NotImplementedError()

    @property
    def base_name(self):
        return self.viewset.base_name

    @property
    def detail_route_name(self):
        return "%s-detail" % self.base_name

    @property
    def list_route_name(self):
        return "%s-list" % self.base_name

    def get_detail_route(self, **kwargs):
        """
        Shortcut for retrieving detail route for this viewset given
        a set of kwargs (e.g. pk=1)
        """
        return reverse(self.detail_route_name, kwargs=kwargs)

    def get_detail_route(self, obj, detail_route_name=None, **kwargs):
        """
        Shortcut for retreiving detail route for a given object.
        Object should be instance of the model which the viewset
        being tested respresents.
        """
        if detail_route_name is None:
            detail_route_name = self.detail_route_name
        return reverse(detail_route_name, kwargs={"pk": obj.pk})

    def get_list_route(self, endpoint=None, params=None, list_route_name=None):
        """
        Shortcut for getting the list route for the API viewset being tested.
        """
        if list_route_name is None:
            list_route_name = self.list_route_name
        route = reverse(list_route_name)
        if endpoint:
            route = '%s%s/' % (route, endpoint)
        if params:
            route = route + '?' + urllib.urlencode(params)
        return route

    def serialize_model(self, obj):
        """
        Serializes `obj` using `self.viewset.serializer_class`. Useful
        for using a factory to construct data which can then be used
        to create new model instances via the viewset being tested.
        """
        exclude = ["url"]
        request_factory = APIRequestFactory()
        request = request_factory.get(self.get_list_route())
        ordered_data = self.viewset.serializer_class(
            instance=obj,
            context={"request": request}
        ).to_representation(obj)
        data = {}
        for key, val in ordered_data.iteritems():
            if key not in exclude:
                data[key] = val
        return data


class APITestCase(_APITestCaseBase):
    """
    Subclass this for testcases which test multiple API viewsets.
    Basically just adds some useful methods which reduce the boilerplate
    required when running tests.
    """
    pass


NUM_OBJS_TO_CREATE = 1


class AutoAPITestsMixin():

    """
    Include this mixin in APIViewSetTestCase instances
    to get some basic tests for free (i.e. tests for
    GET, POST, PUT, PATCH and DELETE).
    """

    submission_format = "json"

    @classproperty
    @classmethod
    def factory(cls):
        raise NotImplementedError()

    def create_user(self):
        User = get_user_model()
        i = 0
        available = False
        while not available:
            username = 'u%d' % i
            try:
                User.objects.get(username=username)
            except User.DoesNotExist:
                available = True
            i += 1
        user = User(username=username)
        user.is_staff = True
        user.is_superuser = True
        user.save()
        user = self.setup_user(user)
        return user

    def setup_user(self, user):
        return user

    def build_obj(self, **kwargs):
        """
        Have copied out and slightly modified the default factory boy
        build method. Benefit is that associated objects are created
        on build, whereas by default they are not.
        NOTE - This method is no longer being used as it doesn't function
        well in the majority of cases. May be useful in future though.
        """
        attrs = self.factory.attributes(create=True, extra=kwargs)
        obj = self.factory._generate(False, attrs)
        return self.setup_obj(obj)

    def create_obj(self):
        obj = self.setup_obj(self.factory.create())
        obj.save()
        return obj

    def setup_obj(self, obj):
        return obj

    def do_auth(self, user=None):
        if user is None:
            user = self.create_user()
        self.user = user
        # TODO - Check that request is initially forbidden
        self.client.force_authenticate(user)

    def test_list(self):
        self.do_auth()
        objs = []
        for _ in range(0, NUM_OBJS_TO_CREATE):
            objs.append(self.create_obj())
        url = self.get_list_route()
        response = self.assertOK(self.client.get(url))
        obj_ids = [o.id for o in objs]
        res_ids = [r['id'] for r in response.data]
        for i in obj_ids:
            self.assertTrue(i in res_ids)

    def test_detail(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        self.assertOK(self.client.get(url))

    def test_create(self):
        self.do_auth()
        url = self.get_list_route()
        obj = self.create_obj()
        data = self.serialize_model(obj)
        obj.delete()
        self.assertCreated(self.client.post(url, data=data, format=self.submission_format))

    def test_update(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        data = self.serialize_model(obj)
        self.assertOK(self.client.put(url, data=data, format=self.submission_format))

    def test_delete(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        self.assertNoContent(self.client.delete(url))


class AutoReadOnlyAPITestsMixin(AutoAPITestsMixin):

    """
    Variation on AutoAPITestsMixin which is suitable for
    readonly viewsets.
    """

    def test_create(self):
        self.do_auth()
        # TODO: Create should respond to a response
        #self.assertMethodNotAllowed(self.client.post(self.get_list_route(), data={}))

    def test_update(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        self.assertMethodNotAllowed(self.client.put(url, data={}))

    def test_delete(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        self.assertMethodNotAllowed(self.client.delete(url))


class DeleteForbiddenMixin():

    def test_delete(self):
        self.do_auth()
        obj = self.create_obj()
        url = self.get_detail_route(obj)
        self.assertForbidden(self.client.delete(url))
