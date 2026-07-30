from mindsdb.integrations.libs.const import HANDLER_TYPE
from .__about__ import __version__ as version, __description__ as description

try:
    from .meta_ads_handler import MetaAdsHandler as Handler
    from .connection_args import connection_args, connection_args_example

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = 'Meta Ads'
name = 'meta_ads'
type = HANDLER_TYPE.DATA
icon_path = 'icon.svg'
permanent = False
__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description',
    'connection_args', 'connection_args_example', 'import_error', 'icon_path'
]
