from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    api_key={"type": ARG_TYPE.PWD, "description": "Optional LangSmith API key. If omitted, SDK environment variables are used.", "required": False, "label": "API key", "secret": True},
    api_url={"type": ARG_TYPE.STR, "description": "Optional LangSmith API URL. If omitted, SDK environment variables are used.", "required": False, "label": "API URL"},
    workspace_id={"type": ARG_TYPE.STR, "description": "Optional LangSmith workspace ID.", "required": False, "label": "Workspace ID"},
    project_name={"type": ARG_TYPE.STR, "description": "Default project name used when queries omit project filters.", "required": False, "label": "Project name"},
)

connection_args_example = OrderedDict(project_name="default")
