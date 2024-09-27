from palace.manager.opds.odl import LicenseInfoDocument


class TestLicenseInfoDocument:
    document = """
    {
 "identifier": "urn:uuid:670d039d-d16c-4dee-b4f5-f37ae5fa54be",
 "status": "available",
 "created": "2023-11-29T19:24:16Z",
 "format": "application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction",
 "price": {
  "value": 72,
  "currency": "usd"
 },
 "terms": {
  "expires": "2025-11-28T00:00:00Z",
  "concurrency": 1,
  "length": 5097600
 },
 "protection": {
  "format": [
   "application/vnd.readium.lcp.license.v1.0+json"
  ],
  "devices": 6,
  "copy": false,
  "print": false,
  "tts": false
 },
 "expires": "2025-11-28T00:00:00Z",
 "checkouts": {
  "available": 1
 }
}
"""

    def test_load(self):
        document = LicenseInfoDocument.model_validate_json(self.document)
        assert document.model_dump_json() == self.document
        # assert document.identifier == "urn:uuid:670d039d-d16c-4dee-b4f5-f37ae5fa54be"
        # assert document.status == LicenseInfoStatus.AVAILABLE
        # assert document.created == datetime(2023, 11, 29, 19, 24, 16, tzinfo=timezone.utc)
        # assert document.format == {"application/audiobook+json; protection=http://www.feedbooks.com/audiobooks/access-restriction"}
        # assert document.price == Price(value=72, currency="usd")
        # assert document.terms == LicenseInfoTerms(expires=datetime(2025, 11, 28, tzinfo=timezone.utc), concurrency=1, length=5097600)
        # assert document.protection == OdlProtection(format={"application/vnd.readium.lcp.license.v1.0+json"}, devices=6, copy=False, print=False, tts=False)
        # assert document.expires == datetime(2025, 11, 28, tzinfo=timezone.utc)
        # assert document.checkouts == LicenseInfoCheckouts(available=1)
