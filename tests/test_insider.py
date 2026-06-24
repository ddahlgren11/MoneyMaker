"""
Tests for insider_ingest.py — the SEC Form 4 parser.

The critical invariant: a real open-market purchase/sale (transaction code P/S)
is correctly extracted with ticker, direction, shares, price, and role; while
non-open-market codes (options, grants, gifts) and sub-threshold trades are
dropped. All offline — no EDGAR calls.
"""
import insider_ingest as ii

# A minimal but realistic Form 4: one open-market purchase (P) by a director,
# one option exercise (M, should be ignored), nested <value> wrappers and an
# issuerTradingSymbol nested under <issuer> (the bug that bit the first cut).
_FORM4_XML = b"""<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>EXAMPLE CORP</issuerName>
    <issuerTradingSymbol>EXC</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>150.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-06-15</value></transactionDate>
      <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionPricePerShare><value>10.00</value></transactionPricePerShare>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

_META = {"accession": "0000320193-26-000001", "cik": "320193", "filed": "2026-06-17"}


def test_parses_open_market_purchase():
    rows = ii._parse_form4(_FORM4_XML, _META)
    assert len(rows) == 1, "only the P transaction should survive (M is dropped)"
    r = rows[0]
    assert r["ticker"] == "EXC"
    assert r["txn_code"] == "P"
    assert r["direction"] == "Up"
    assert r["shares"] == 1000
    assert r["price"] == 150.0
    assert r["value"] == 150000.0
    assert r["txn_date"] == "2026-06-15"
    assert r["disclosure_date"] == "2026-06-17"


def test_role_and_insider_extracted():
    r = ii._parse_form4(_FORM4_XML, _META)[0]
    assert r["insider"] == "Doe Jane"
    assert "Director" in r["role"]
    assert "CEO" in r["role"]


def test_sale_maps_to_down():
    xml = _FORM4_XML.replace(b"<transactionCode>P</transactionCode>",
                             b"<transactionCode>S</transactionCode>")
    r = ii._parse_form4(xml, _META)[0]
    assert r["direction"] == "Down"


def test_sub_threshold_value_dropped(monkeypatch):
    monkeypatch.setattr(ii, "INSIDER_MIN_VALUE", 1_000_000)
    assert ii._parse_form4(_FORM4_XML, _META) == []


def test_direction_map_only_p_and_s():
    assert ii._DIRECTION_BY_CODE == {"P": "Up", "S": "Down"}


def test_ticker_validation():
    assert ii._ticker_ok("AAPL")
    assert ii._ticker_ok("BRK.B")
    assert not ii._ticker_ok("")
    assert not ii._ticker_ok("TOOLONGSYM")


def test_dedup_key_stable_and_unique():
    rows = ii._parse_form4(_FORM4_XML, _META)
    k1 = ii._dedup_key(rows[0])
    k2 = ii._dedup_key(rows[0])
    assert k1 == k2
    other = dict(rows[0], shares=999)
    assert ii._dedup_key(other) != k1


def test_malformed_xml_returns_empty():
    assert ii._parse_form4(b"<not valid", _META) == []
