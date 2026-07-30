from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    ad_account_id={
        "type": ARG_TYPE.STR,
        "description": (
            "Meta Ads account id, without the 'act_' prefix (e.g. 1234567890). "
            "The handler builds act_<id> itself."
        ),
        "label": "Ad Account ID",
        "required": True,
    },
    access_token={
        "type": ARG_TYPE.STR,
        "description": "Long-lived Meta Marketing API user or system-user access token.",
        "label": "Access Token",
        "required": True,
        "secret": True,
    },
    api_version={
        "type": ARG_TYPE.STR,
        "description": "Meta Graph API version used for requests. Defaults to v25.0.",
        "label": "API Version",
        "required": False,
    },
    client_id={
        "type": ARG_TYPE.STR,
        "description": "Meta app id. Informational only, never sent on requests.",
        "label": "Client ID",
        "required": False,
    },
    client_secret={
        "type": ARG_TYPE.STR,
        "description": "Meta app secret. When provided, it is used to compute appsecret_proof.",
        "label": "Client Secret",
        "required": False,
        "secret": True,
    },
)

connection_args_example = OrderedDict(
    ad_account_id="1234567890",
    access_token="your_access_token_here",
    api_version="v25.0",
    client_id="1234567890123456",
    client_secret="your_client_secret_here",
)
