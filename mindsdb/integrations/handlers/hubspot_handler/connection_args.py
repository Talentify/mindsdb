from collections import OrderedDict
from mindsdb.integrations.handlers.hubspot_handler.__about__ import __version__ as version
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    # Token Injection Parameters (primary method for backend integration)
    access_token={
        'type': ARG_TYPE.STR,
        'description': 'HubSpot access token (for token injection from backend systems)',
        'label': 'Access Token',
        'required': False,
        'secret': True,
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'HubSpot refresh token (for automatic token refresh)',
        'label': 'Refresh Token',
        'required': False,
        'secret': True,
    },

    # OAuth2 Application Credentials
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'HubSpot OAuth2 Client ID (required for token refresh)',
        'label': 'OAuth Client ID',
        'required': False,
        'secret': True,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'HubSpot OAuth2 Client Secret (required for token refresh)',
        'label': 'OAuth Client Secret',
        'required': False,
        'secret': True,
    },

    # Optional Parameters
    hub_id={
        'type': ARG_TYPE.STR,
        'description': 'HubSpot Hub ID (Portal ID). If not provided, will be extracted from token info.',
        'label': 'Hub ID',
        'required': False,
    },

    # OAuth2 Code Flow Parameters (for future use)
    code={
        'type': ARG_TYPE.STR,
        'description': 'Authorization code obtained from OAuth flow (code flow only)',
        'label': 'Authorization Code',
        'required': False,
        'secret': True,
    },
    redirect_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth2 Redirect URI (must match your HubSpot app configuration). Required for code flow.',
        'label': 'Redirect URI',
        'required': False,
    },
)

connection_args_example = OrderedDict(
    access_token='your_access_token_here',
    refresh_token='your_refresh_token_here',
    client_id='your_client_id_here',
    client_secret='your_client_secret_here',
)
