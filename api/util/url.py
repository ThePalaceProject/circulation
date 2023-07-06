import re
from urllib.parse import ParseResult, urlencode, urlparse

from core.config import Configuration


class URLUtility:
    """Contains different helper methods simplifying URL construction."""

    @staticmethod
    def build_url(base_url, query_parameters):
        """Construct a URL with specified query parameters.

        :param base_url: Base URL
        :type base_url: str

        :param query_parameters: Dictionary containing query parameters
        :type query_parameters: Dict

        :return: Constructed URL
        :rtype: str
        """
        result = urlparse(base_url)
        result = ParseResult(
            result.scheme,
            result.netloc,
            result.path,
            result.params,
            urlencode(query_parameters),
            result.fragment,
        )

        return result.geturl()


class CDNUtils:
    @classmethod
    def replace_host(cls, url: str) -> str:
        cdn_url = Configuration.cdn_base_url()
        if url.startswith("http") and cdn_url:
            # Find the hostname to replace
            replace = re.compile("(^https?://.*?)/")
            matches = replace.match(url)
            if matches:
                # Replace the group
                url = url.replace(matches.group(1), cdn_url)

        return url
