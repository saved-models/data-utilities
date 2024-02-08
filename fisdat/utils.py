def fst(g):
    """
    Utility function. RDFLib gives generators all over the place and
    usually we just want a single value.
    """
    for e in g:
        return e
    raise Exception("Generator is empty")
