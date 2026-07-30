from __future__ import annotations

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource

from .utils import _to_numeric


class AccountTable(APIResource):
    """Meta Ads account. GET /{account_path}. Single-row table: always returns exactly
    one row, and a fetch failure propagates rather than returning an empty DataFrame.

    amount_spent and spend_cap are in the account's currency minor units (cents).
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
    ]

    NUMERIC_COLUMNS = ["account_status", "timezone_offset_hours_utc", "amount_spent", "spend_cap"]

    def get_columns(self) -> list[str]:
        return self.COLUMNS

    def list(self, conditions=None, limit=None, sort=None, targets=None, **kwargs):
        fields = ",".join(self.COLUMNS)
        # Let a fetch failure propagate: an empty account row is not acceptable.
        row = self.handler.graph_get(self.handler.account_path, {"fields": fields})

        df = pd.DataFrame([row])
        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = None

        df = _to_numeric(df, self.NUMERIC_COLUMNS)

        return df[self.COLUMNS]
