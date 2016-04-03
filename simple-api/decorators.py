
class classproperty(property):
    """ Similar to the standard python `@property` decorator,
    but for classes. Must be used above the `@classmethod`
    decorator, e.g.:

        ```python

        @classproperty
        @classmethod
        def my_class_property(cls):
            return "banana!"

        ```
    """

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()
