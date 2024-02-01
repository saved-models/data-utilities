from __future__ import annotations

import sys
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, urlopen
from urllib.response import addinfourl


def _urlopen(request: Request) -> addinfourl:
    """
    This is a shim for `urlopen` that handles HTTP redirects with status code
    308 (Permanent Redirect).

    This function should be removed once all supported versions of Python
    handles the 308 HTTP status code.

    :param request: The request to open.
    :return: The response to the request.
    """
    try:
        return urlopen(request, verify=False)
    except HTTPError as error:
        if error.code == 308 and sys.version_info < (3, 11):
            # HTTP response code 308 (Permanent Redirect) is not supported by python
            # versions older than 3.11. See <https://bugs.python.org/issue40321> and
            # <https://github.com/python/cpython/issues/84501> for more details.
            # This custom error handling should be removed once all supported
            # versions of Python handles 308.
            new_request = _make_redirect_request(request, error)
            return _urlopen(new_request)
        else:
            raise
