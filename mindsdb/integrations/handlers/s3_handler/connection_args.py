from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The AWS access key that identifies the user or IAM role. Opcional se usar IAM Role via ServiceAccount.',
        'required': False,
        'label': 'AWS Access Key'
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The AWS secret access key que identifica o usuário ou IAM role. Opcional se usar IAM Role via ServiceAccount.',
        'secret': True,
        'required': False,
        'label': 'AWS Secret Access Key'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Amazon S3 bucket.',
        'required': True,
        'label': 'Amazon S3 Bucket'
    },
    path_prefix={
        'type': ARG_TYPE.STR,
        'description': 'Optional S3 key prefix used to pre-filter objects returned by the files table.',
        'required': False,
        'label': 'S3 Path Prefix'
    },
    prefix={
        'type': ARG_TYPE.STR,
        'description': 'Alias for path_prefix. Optional S3 key prefix used to pre-filter objects returned by the files table.',
        'required': False,
        'label': 'S3 Prefix'
    },
    include_metadata={
        'type': ARG_TYPE.BOOL,
        'description': 'Whether to include custom S3 object metadata in the files table. Defaults to false and requires one HeadObject request per file when enabled.',
        'required': False,
        'label': 'Include Object Metadata'
    },
    list_cache_ttl_seconds={
        'type': ARG_TYPE.INT,
        'description': 'Time-to-live in seconds for cached S3 object listings used by the files table. Defaults to 300 seconds.',
        'required': False,
        'label': 'List Cache TTL Seconds'
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region to connect to. Default is `us-east-1`.',
        'required': False,
        'label': 'AWS Region'
    },
    aws_session_token={
        'type': ARG_TYPE.STR,
        'description': 'The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.',
        'secret': True,
        'required': False,
        'label': 'AWS Session Token'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='AQAXEQK89OX07YS34OP',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    aws_session_token='FQoGZXIvYXdzEHcaDmJjJj...',
    region_name='us-east-2',
    bucket='my-bucket',
    path_prefix='rules/country=US/',
    prefix='rules/country=US/',
    include_metadata=False,
    list_cache_ttl_seconds=300,
)
