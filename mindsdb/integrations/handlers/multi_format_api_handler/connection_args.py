from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    url={
        'type': ARG_TYPE.STR,
        'description': 'Default URL for API endpoint. If specified, queries do not need to include a URL in the WHERE clause.',
        'required': False,
        'label': 'Default URL',
    },
    headers={
        'type': ARG_TYPE.DICT,
        'description': 'Default HTTP headers as a dictionary. Used for authentication (e.g., API keys, Bearer tokens).',
        'required': False,
        'label': 'Default Headers',
    },
    method={
        'type': ARG_TYPE.STR,
        'description': 'HTTP method to use: GET or POST. Default is GET.',
        'required': False,
        'label': 'HTTP Method',
    },
    body={
        'type': ARG_TYPE.DICT,
        'description': 'Default request body sent with POST requests as JSON.',
        'required': False,
        'label': 'Default Request Body',
    },
    timeout={
        'type': ARG_TYPE.INT,
        'description': 'Default request timeout in seconds. Default is 30 seconds.',
        'required': False,
        'label': 'Default Timeout',
    },
    max_content_size={
        'type': ARG_TYPE.INT,
        'description': 'Maximum response size in MB. Prevents downloading extremely large files. Default is 100 MB.',
        'required': False,
        'label': 'Max Content Size (MB)',
    },
    record_path={
        'type': ARG_TYPE.STR,
        'description': (
            'Dot-path to the array of records inside a JSON response (e.g. "tickers" '
            'or "data.results"). Use to override auto-detection when a payload has '
            'ambiguous or nested record arrays. Leave empty to auto-detect.'
        ),
        'required': False,
        'label': 'JSON Record Path',
    },
    auto_explode={
        'type': ARG_TYPE.STR,
        'description': (
            "Whether to explode a JSON record array into rows ('true'/'false'). "
            "Default 'true'. Set to 'false' to keep the legacy single-row shape "
            'for object payloads.'
        ),
        'required': False,
        'label': 'Auto-Explode JSON Arrays',
    },
)


connection_args_example = OrderedDict(
    url='https://api.talentify.io/linkedin-slots/feed',
    headers={
        'Authorization': 'Bearer YOUR_TOKEN_HERE',
        'X-API-Key': 'your-api-key',
    },
    method='POST',
    body={'query': 'search term', 'variables': {}},
    timeout=60,
    max_content_size=50,  # 50 MB limit
)
