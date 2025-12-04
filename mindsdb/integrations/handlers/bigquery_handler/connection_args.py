from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    project_id={
        'type': ARG_TYPE.STR,
        'description': 'The BigQuery project id.'
    },
    dataset={
        'type': ARG_TYPE.STR,
        'description': 'The BigQuery dataset name.'
    },
    service_account_keys={
        'type': ARG_TYPE.PATH,
        'description': 'Full path or URL to the service account JSON file',
        'secret': True
    },
    service_account_json={
        'type': ARG_TYPE.DICT,
        'description': 'Content of service account JSON file',
        'secret': True
    },
    include_tables={
        'type': ARG_TYPE.STR,
        'description': 'Comma-separated list of table names to include. Only these tables will be accessible.',
        'required': False,
        'label': 'Include Tables'
    },
    exclude_tables={
        'type': ARG_TYPE.STR,
        'description': 'Comma-separated list of table names to exclude. Applied after include_tables.',
        'required': False,
        'label': 'Exclude Tables'
    },
)

connection_args_example = OrderedDict(
    project_id='tough-future-332513',
    service_account_keys='/home/bigq/tough-future-332513.json'
)
