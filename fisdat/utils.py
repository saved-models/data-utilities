def fst(g):
    """
    Utility function. RDFLib gives generators all over the place and
    usually we just want a single value.
    """
    for e in g:
        return e
    raise Exception("Generator is empty")

class error(object):
    _strict = False
    @classmethod
    def strict(cls, strict):
        cls._strict = strict
    def __init__(self, s):
        """
        Raise an exception if we are in strict mode, otherwise just print
        things like validation errors.
        """
        if self._strict:
            raise Exception(s)
        else:
            print(s)
