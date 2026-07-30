from __future__ import annotations

from typing import Optional, Tuple


# Insights-specific (code, error_subcode) taxonomy from
# tasks/meta-ads-api-research/error-codes.md. Keyed on the *pair*, not the subcode
# alone -- the subcode value space is not guaranteed unique across codes, and Meta's
# own error-codes table keys on the pair.
INSIGHTS_RETRYABLE = {(4, 1504022), (4, 1504039), (2, 1504043), (2, 1504044), (-2, 2490547)}
INSIGHTS_NON_RETRYABLE = {(2, 1504041), (2, 1504042), (100, 3191001)}
INSIGHTS_LARGE_REQUEST = {(100, 1487534), (-3, 1504045), (100, 1504018), (2, 1504038)}


def to_int(value) -> Optional[int]:
    """Best-effort int coercion that never raises. error_subcode (and occasionally
    code) can arrive as a string in real payloads."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def error_code_pair(error_info: Optional[dict]) -> Optional[Tuple[int, int]]:
    """Normalise error_info's (code, error_subcode) into a comparable int pair.

    Returns None -- never raises -- when error_info is absent, error_subcode is
    missing, or either value can't be coerced to int. Callers should treat None as
    "does not match any documented (code, subcode) set" rather than an error.
    """
    if not error_info:
        return None
    code = to_int(error_info.get("code"))
    subcode = to_int(error_info.get("error_subcode"))
    if code is None or subcode is None:
        return None
    return (code, subcode)


def is_large_request_error(error_info: Optional[dict]) -> bool:
    """Shared predicate for Meta's oversized-report / large-request errors.

    Checks the documented Insights INSIGHTS_LARGE_REQUEST (code, error_subcode) pairs
    first. Then falls back to the pre-existing undocumented signals -- error.code == 1
    with a "reduce the amount of data" message, or error_subcode == 99 -- which are not
    in Meta's published taxonomy but were presumably added against observed behaviour;
    this audit proved the docs are incomplete in exactly this way, so they stay as an
    additional fallback rather than being removed.

    Operates on the *parsed* error dict (code/error_subcode/message) rather than a
    formatted message string, so it can't accidentally match text that only happens
    to appear inside error_user_msg or some other free-form field.
    """
    if not error_info:
        return False
    pair = error_code_pair(error_info)
    if pair is not None and pair in INSIGHTS_LARGE_REQUEST:
        return True
    if to_int(error_info.get("error_subcode")) == 99:
        return True
    # to_int here too: code arrives as a string in some payloads, and a raw `== 1`
    # would silently skip this fallback for exactly those.
    code = to_int(error_info.get("code"))
    message = str(error_info.get("message") or "").lower()
    return code == 1 and "reduce the amount of data" in message


class MetaAdsAPIError(RuntimeError):
    """Raised for a non-2xx Graph API response.

    Carries the parsed `error` dict (when the body was JSON) alongside the formatted
    message, so callers -- e.g. InsightsTable's large-request async fallback -- can
    make routing decisions directly on the structured fields instead of re-parsing the
    formatted string.
    """

    def __init__(self, message: str, error_info: Optional[dict] = None):
        super().__init__(message)
        self.error_info = error_info

    def is_large_request_error(self) -> bool:
        return is_large_request_error(self.error_info)
