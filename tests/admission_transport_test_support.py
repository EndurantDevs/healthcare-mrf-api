# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared request helpers use the actual composed writer-authority contract."""

import ssl
from dataclasses import replace

from process.custom_import.build_source import SourceBuildRequest as AuthorizedRequest

# Public certificate only, with no retained private key or deployment identity.
WRITER_CA_PEM = b"""-----BEGIN CERTIFICATE-----
MIIC7DCCAdSgAwIBAgIBATANBgkqhkiG9w0BAQsFADAuMSwwKgYDVQQDDCNzeW50
aGV0aWMtd3JpdGVyLWNhLmV4YW1wbGUuaW52YWxpZDAgFw0yMDAxMDEwMDAwMDBa
GA8yMTAwMDEwMTAwMDAwMFowLjEsMCoGA1UEAwwjc3ludGhldGljLXdyaXRlci1j
YS5leGFtcGxlLmludmFsaWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDM0NO2WRxE/hYcneH0TLUWmQ2bDKKDOLHylNngOcs4ngS+uybMc082pIjJhw1j
KjZw2/j/J8wvg2AsudIpu3Nq6dIFquITTxTVCAVZKw2jz0I3k3mqwZgStZw8NdAo
jZfysl6zKpumKTujtC1riPpj7oo4ZVzAN2EXW5Ybrm1dEVDA6+tu5IzPIsUtBOof
1XDhF+vuk8/tA1Tj+S0VdkV8DcFYJUP7Ek/TlDsQ/yQuVqYdUxKBneBvhEsfmPCn
TI5v//Cpw9enhlT8ADJzQZx+0JCIN8WhbEtGt/JztRO1bLdQX/xLrz6AOcL17EO3
DYhr8lhqhlQGxNnfe/70Ov/bAgMBAAGjEzARMA8GA1UdEwEB/wQFMAMBAf8wDQYJ
KoZIhvcNAQELBQADggEBAMs2oW07zGpVNx74IZz/BQWLlnbWE/PkpA6kXagpUeHU
4G5LlpZKFmQGGiFcesWe15NFazpGECcWOydI+XOiFA4XkISyn6rnmHe7abprH/48
iDSnVPFTP9iKVIcbpzkkAXTH0vh6ldeBsax4AzdEsluu+WoKj4QO8r8KiaPKn6gZ
TMD9NMRryJC+x+T0cMy1CjnVPvLZIGVnoJ1OCcYEbsl7aesvjQSvkYG8ZW47PH5J
ZS1MUHSs27g3cD5yUp+/dqC5s4B4qwHGg0zm8yugaOySQYpyhK7jJ0eDbsLuNtH/
1nAoHB10/2nUgYzNpeiaFjEhcForcB5OEVsfWR2ZqFY=
-----END CERTIFICATE-----
"""


def writer_tls_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cadata=WRITER_CA_PEM.decode("ascii"))
    return context


def authorized_request(original, **changes):
    return replace(original, **changes)
