"""
Single source of truth for an order's normalized landing path.

V1 collection/PLP attribution model (NOT session-level ground truth):
  landing_site  - Shopify's own first-visit URL (native-checkout orders)
  full_url      - note attribute set by the store's checkout script
                  (GoKwik-checkout orders)
The fallback is applied per ORDER, never per brand.

Normalization: drop scheme + host, ?query and #hash, percent-decode, collapse
repeated slashes, lowercase, strip the trailing slash ("/" stays "/").
Percent-decoding matters: orders carry the encoded form
(/collections/%F0%9F%94%A5-sale) while Shopify's session data carries the
decoded form (/collections/<emoji>-sale), and the two must match. Anything that
is not an absolute URL, a protocol-relative URL or a "/path" is unparseable.

Not handled (V1 limitation): locale prefixes such as /en/collections/x are kept
as-is, so they are not exact collection landings.
"""

import re
from typing import Optional
from urllib.parse import unquote

_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")

# Exactly one non-empty handle segment: /collections/<handle>.
# /collections/, /collections/x/products/y, /products/x, /, /cart are not.
COLLECTION_PATH_RE = re.compile(r"^/collections/[^/]+$")


def normalize_landing_path(url) -> Optional[str]:
    if url is None:
        return None
    s = str(url).strip()
    if not s:
        return None
    s = s.split("#", 1)[0].split("?", 1)[0]

    if _SCHEME_RE.match(s):
        rest = s[s.index("://") + 3 :]
        slash = rest.find("/")
        s = rest[slash:] if slash != -1 else "/"
    elif s.startswith("//"):
        rest = s[2:]
        slash = rest.find("/")
        s = rest[slash:] if slash != -1 else "/"
    elif not s.startswith("/"):
        return None

    s = unquote(s)
    s = re.sub(r"/{2,}", "/", s).lower()
    if len(s) > 1:
        s = s.rstrip("/")
    return s or "/"


def pick_landing_path(landing_site, full_url) -> Optional[str]:
    """landing_site if it yields a path, else full_url, else None."""
    for candidate in (landing_site, full_url):
        path = normalize_landing_path(candidate)
        if path:
            return path
    return None


def is_collection_landing_path(path: Optional[str]) -> bool:
    return bool(path) and COLLECTION_PATH_RE.match(path) is not None
