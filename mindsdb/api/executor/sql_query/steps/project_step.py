from collections import defaultdict

from mindsdb_sql_parser.ast import (
    Identifier,
    Select,
    Star,
)
from mindsdb.api.executor.planner.steps import ProjectStep
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.api.executor.exceptions import (
    KeyColumnDoesNotExist,
    NotSupportedYet
)

from .base import BaseStepCall
from .fetch_dataframe import get_fill_param_fnc


class ProjectStepCall(BaseStepCall):

    bind = ProjectStep

    def call(self, step):
        result_set = self.steps_data[step.dataframe.step_num]

        df, col_names = result_set.to_df_cols()
        col_idx = {}
        tbl_idx = defaultdict(list)
        for name, col in col_names.items():
            col_idx[col.alias] = name
            col_idx[(col.table_alias, col.alias)] = name
            # add to tables
            tbl_idx[col.table_name].append(name)
            if col.table_name != col.table_alias:
                tbl_idx[col.table_alias].append(name)

        # Resolve Parameter nodes from previous step results
        fill_params = get_fill_param_fnc(self.steps_data)
        resolved_columns = query_traversal(step.columns, fill_params) or step.columns

        # analyze condition and change name of columns
        def check_fields(node, is_table=None, **kwargs):
            if is_table:
                return  # skip table nodes — subqueries already resolved to constants
            if isinstance(node, Identifier):
                # only column name
                col_name = node.parts[-1]
                if isinstance(col_name, Star):
                    if len(node.parts) == 1:
                        # left as is
                        return
                    else:
                        # replace with all columns from table
                        table_name = node.parts[-2]
                        return [
                            Identifier(parts=[col])
                            for col in tbl_idx.get(table_name, [])
                        ]

                if len(node.parts) == 1:
                    key = col_name
                else:
                    table_name = node.parts[-2]
                    key = (table_name, col_name)

                if key not in col_idx:
                    simple_cols = [k for k in col_idx.keys() if isinstance(k, str)]
                    raise KeyColumnDoesNotExist(
                        f'Column not found: {key}.\n'
                        f'Available columns: {", ".join(str(c) for c in simple_cols[:20])}'
                        + (f' ... and {len(simple_cols) - 20} more' if len(simple_cols) > 20 else '')
                    )

                new_name = col_idx[key]
                return Identifier(parts=[new_name], alias=node.alias)

        query = Select(
            targets=resolved_columns,
            from_table=Identifier('df_table')
        )

        targets0 = query_traversal(query.targets, check_fields)
        targets = []
        for target in targets0:
            if isinstance(target, list):
                targets.extend(target)
            else:
                targets.append(target)
        query.targets = targets

        res = query_df(df, query, session=self.session)

        return ResultSet.from_df_cols(df=res, columns_dict=col_names, strict=False)
