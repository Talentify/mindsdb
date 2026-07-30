from __future__ import annotations

from typing import Optional


def is_large_request_error(error_info: Optional[dict]) -> bool:
    """Shared predicate for Meta's oversized-report error (error.code == 1 with a
    "reduce the amount of data" message, also seen as error_subcode 99).

    Operates on the *parsed* error dict (code/error_subcode/message) rather than a
    formatted message string, so it can't accidentally match text that only happens
    to appear inside error_user_msg or some other free-form field.
    """
    if not error_info:
        return False
    if error_info.get("error_subcode") == 99:
        return True
    code = error_info.get("code")
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
