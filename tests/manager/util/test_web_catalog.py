import pytest

from palace.manager.util.web_catalog import promote_label, with_promoted_libraries


class TestWebCatalog:
    @pytest.mark.parametrize(
        "url, libraries, expected",
        [
            pytest.param(
                "https://catalog.example.com",
                [],
                "https://catalog.example.com",
                id="no-libraries-leaves-url-unchanged",
            ),
            pytest.param(
                "https://catalog.example.com",
                ["lib-a", "lib-b"],
                "https://catalog.example.com?promote=lib-a,lib-b",
                id="libraries",
            ),
            pytest.param(
                "https://catalog.example.com/?lang=fr&empty=&flag&sp=a%20b&bad=%zz#top",
                ["lib-a"],
                "https://catalog.example.com/"
                "?lang=fr&empty=&flag&sp=a%20b&bad=%zz&promote=lib-a#top",
                id="other-query-and-fragment-are-kept-exactly",
            ),
            pytest.param(
                "https://catalog.example.com/?a=1&&b=2&",
                ["lib-a"],
                "https://catalog.example.com/?a=1&b=2&promote=lib-a",
                id="empty-query-pairs-are-dropped",
            ),
            pytest.param(
                "https://catalog.example.com/?promote=old&x=1&%70romote=older",
                ["lib-a"],
                "https://catalog.example.com/?x=1&promote=lib-a",
                id="existing-promoted-parameters-are-replaced",
            ),
            pytest.param(
                "https://catalog.example.com/?promoteLabel=Old&promoteOrder=listed",
                ["lib-a"],
                "https://catalog.example.com/?promoteOrder=listed&promote=lib-a",
                id="existing-label-is-removed-and-other-promote-settings-are-kept",
            ),
            pytest.param(
                "https://catalog.example.com/?promote=old&promoteLabel=Old&x=1",
                [],
                "https://catalog.example.com/?x=1",
                id="no-libraries-removes-existing-promote-parameters",
            ),
            pytest.param(
                "https://catalog.example.com",
                ["a&b", "c d"],
                "https://catalog.example.com?promote=a%26b,c+d",
                id="short-names-are-encoded",
            ),
        ],
    )
    def test_with_promoted_libraries(
        self, url: str, libraries: list[str], expected: str
    ) -> None:
        assert with_promoted_libraries(url, libraries) == expected

    @pytest.mark.parametrize(
        "url, libraries, label, expected",
        [
            pytest.param(
                "https://catalog.example.com/?promoteLabel=Old",
                ["lib-a"],
                "Our schools & more",
                "https://catalog.example.com/"
                "?promote=lib-a&promoteLabel=Our+schools+%26+more",
                id="label-replaces-existing-label",
            ),
            pytest.param(
                "https://catalog.example.com",
                [],
                "Our schools",
                "https://catalog.example.com",
                id="label-is-not-sent-without-libraries",
            ),
            pytest.param(
                "https://catalog.example.com",
                ["lib-a"],
                "",
                "https://catalog.example.com?promote=lib-a",
                id="empty-label-is-not-sent",
            ),
        ],
    )
    def test_with_promoted_libraries_label(
        self, url: str, libraries: list[str], label: str, expected: str
    ) -> None:
        assert with_promoted_libraries(url, libraries, label) == expected

    @pytest.mark.parametrize(
        "url, expected",
        [
            pytest.param(
                "https://catalog.example.com/?promoteLabel=Our+schools+%26+more",
                "Our schools & more",
                id="label-is-decoded",
            ),
            pytest.param(
                "https://catalog.example.com/?promoteLabel=+&promoteLabel=+Ours+",
                "Ours",
                id="first-non-blank-label-is-stripped",
            ),
            pytest.param(
                "https://catalog.example.com/?promoteLabel=",
                None,
                id="empty-label",
            ),
            pytest.param("https://catalog.example.com/?x=1", None, id="no-label"),
        ],
    )
    def test_promote_label(self, url: str, expected: str | None) -> None:
        assert promote_label(url) == expected
