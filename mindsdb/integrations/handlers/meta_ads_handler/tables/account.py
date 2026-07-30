from __future__ import annotations

import json

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _to_numeric


class AccountTable(APIResource):
    """Meta Ads account. GET /{account_path}. Single-row table: always returns exactly
    one row, and a fetch failure propagates rather than returning an empty DataFrame.

    amount_spent and spend_cap are returned by Graph in the account's currency minor
    units. Meta defines a per-currency offset controlling this: offset 100 (the
    common case, e.g. USD) means the value is expressed in 1/100ths of the base unit
    (divide by 100 to get base units, e.g. cents to dollars); offset 1 (CLP, COP, CRC,
    HUF, ISK, IDR, JPY, KRW, PYG, TWD, VND) means the value already is the base unit,
    no division needed. Use `currency` (this table) to know which applies. We do not
    scale these values today.
    """

    COLUMNS = [
        "id",
        "account_id",
        "name",
        "account_status",
        "currency",
        "timezone_name",
        "timezone_offset_hours_utc",
        "business_name",
        "amount_spent",
        "spend_cap",
        "funding_source",
        "created_time",
        "balance",
        "disable_reason",
        "min_daily_budget",
        "opportunity_score",
        "capabilities",
        "end_advertiser_name",
        "timezone_id",
        "age",
        "is_prepay_account",
        "tax_id_status",
        # Enum-code labels, added alongside (not replacing) the raw codes above.
        "account_status_label",
        "tax_id_status_label",
        "disable_reason_label",
    ]

    # Live-measured wire types (recorded here since they don't match what the docs'
    # bare type names would suggest): `balance` arrives as a numeric string (e.g.
    # "23799"), not an int; `age` is a float (e.g. 1981.2596643519), not an int.
    # `_to_numeric` (pd.to_numeric) handles both without special-casing.
    # `account_status`/`tax_id_status` arrive as JSON ints (not numeric strings) --
    # `_label_for`'s `int(raw_value)` cast already tolerates either, so no change
    # was needed there.
    NUMERIC_COLUMNS = [
        "account_status",
        "timezone_offset_hours_utc",
        "amount_spent",
        "spend_cap",
        "balance",
        "disable_reason",
        "min_daily_budget",
        "opportunity_score",
        "timezone_id",
        "age",
        "tax_id_status",
    ]
    JSON_COLUMNS = ["capabilities"]

    # account_status: fully documented in tasks/meta-ads-api-research/ad-account.md.
    ACCOUNT_STATUS_LABELS = {
        1: "ACTIVE",
        2: "DISABLED",
        3: "UNSETTLED",
        7: "PENDING_RISK_REVIEW",
        8: "PENDING_SETTLEMENT",
        9: "IN_GRACE_PERIOD",
        100: "PENDING_CLOSURE",
        101: "CLOSED",
        201: "ANY_ACTIVE",
        202: "ANY_CLOSED",
    }
    # tax_id_status: fully documented in tasks/meta-ads-api-research/ad-account.md.
    TAX_ID_STATUS_LABELS = {
        0: "UNKNOWN",
        1: "NOT_REQUIRED_US_CA",
        2: "REQUIRED",
        3: "SUBMITTED",
        4: "OFFLINE_VALIDATION_FAILED",
        5: "PERSONAL_ACCOUNT",
    }
    # disable_reason: only the two endpoints of the range are documented
    # (0=NONE ... 15=COMPROMISED_AD_ACCOUNT) -- codes 1-14 are undocumented gaps, not
    # an oversight. Map only what's confirmed; every other code resolves to None via
    # _label_for rather than a guessed label.
    DISABLE_REASON_LABELS = {
        0: "NONE",
        15: "COMPROMISED_AD_ACCOUNT",
    }

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    # Real Graph field names; the *_label columns are derived client-side and would
    # 400 the request if sent as-is.
    _DERIVED_COLUMNS = ["account_status_label", "tax_id_status_label", "disable_reason_label"]
    _REQUEST_FIELDS = []
    for _col in COLUMNS:
        if _col not in _DERIVED_COLUMNS:
            _REQUEST_FIELDS.append(_col)
    del _col

    @staticmethod
    def _label_for(mapping: dict, raw_value) -> str | None:
        # Graph may return the code as an int or as a numeric string; normalize
        # before the dict lookup rather than requiring an exact type match.
        try:
            code = int(raw_value)
        except (TypeError, ValueError):
            return None
        return mapping.get(code)

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        fields = ",".join(self._REQUEST_FIELDS)
        # Let a fetch failure propagate: an empty account row is not acceptable.
        row = self.handler.graph_get(self.handler.account_path, {"fields": fields})

        row["account_status_label"] = self._label_for(self.ACCOUNT_STATUS_LABELS, row.get("account_status"))
        row["tax_id_status_label"] = self._label_for(self.TAX_ID_STATUS_LABELS, row.get("tax_id_status"))
        row["disable_reason_label"] = self._label_for(self.DISABLE_REASON_LABELS, row.get("disable_reason"))

        df = pd.DataFrame([row])
        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        for column in self.JSON_COLUMNS:
            df[column] = df[column].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
        df = _to_numeric(df, self.NUMERIC_COLUMNS)

        return df[self.COLUMNS]
