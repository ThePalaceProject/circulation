from urllib.parse import ParseResult, urlencode, urlparse


class URLUtility:
    """Contains different helper methods simplifying URL construction."""

    @staticmethod
    def build_url(base_url: str, query_parameters: dict[str, str]) -> str:
        """Construct a URL with specified query parameters.

        :param base_url: Base URL

        :param query_parameters: Dictionary containing query parameters

        :return: Constructed URL
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
