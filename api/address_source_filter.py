# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Filter exact addresses using their already materialized source count."""

from sanic.exceptions import InvalidUsage
from sqlalchemy import Integer, false, literal_column, true

ADDRESS_SOURCE_FILTER_HEADER = "X-HealthPorta-Address-Source-Filter"
ADDRESS_SOURCE_FILTER_CONTRACT = "v1"


def acknowledge_address_source_filter(request, response):
    """Confirm this operation's negotiated filter contract during mixed releases."""
    response.headers.popall(ADDRESS_SOURCE_FILTER_HEADER, None)
    if 200 <= response.status < 300 and request.headers.get_all(ADDRESS_SOURCE_FILTER_HEADER) == [
        ADDRESS_SOURCE_FILTER_CONTRACT
    ]:
        response.headers[ADDRESS_SOURCE_FILTER_HEADER] = ADDRESS_SOURCE_FILTER_CONTRACT


def parse_min_address_sources(args):
    """Accept one bounded integer without interpreting missing evidence as support."""
    name = "min_address_sources"
    missing = object()
    value = args.get(name, missing)
    if value is missing:
        return 0
    if hasattr(args, "getlist") and len(args.getlist(name)) != 1:
        raise InvalidUsage(f"{name} must be supplied once")
    if type(value) is str and value.isascii() and value.isdecimal() and len(value) <= 10:
        value = int(value)
    if type(value) is not int or not 0 <= value <= 2147483647:
        raise InvalidUsage(f"{name} must be an integer between 0 and 2147483647")
    return value


def address_source_filter_sql(alias, minimum, *, has_source_count=True):
    """Expose validated integer selectivity to prepared-query planning."""
    minimum = parse_min_address_sources({"min_address_sources": minimum})
    if not minimum:
        return "TRUE"
    if not has_source_count:
        return "FALSE"
    return f"{alias}.source_count >= {minimum}"


def address_source_filter(table, minimum):
    """Use the same eligibility rule for SQLAlchemy address queries."""
    minimum = parse_min_address_sources({"min_address_sources": minimum})
    if not minimum:
        return true()
    if "source_count" not in table.c:
        return false()
    return table.c.source_count >= literal_column(str(minimum), type_=Integer())


def filter_address_source_rows(addresses_by_npi, minimum, *, member_npis=None):
    """Capture original membership, then filter persisted rows before any deduplication."""
    if member_npis is not None:
        member_npis.update(npi for npi, addresses in addresses_by_npi.items() if addresses)
    if not minimum:
        return addresses_by_npi
    return {
        npi: [
            address
            for address in addresses
            if type(address.get("source_count")) is int and address["source_count"] >= minimum
        ]
        for npi, addresses in addresses_by_npi.items()
    }
