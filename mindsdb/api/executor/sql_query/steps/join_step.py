import copy

import numpy as np

from mindsdb_sql_parser.ast import (
    Identifier, BinaryOperation, Constant
)
from mindsdb.api.executor.planner.steps import (
    JoinStep,
)
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb.api.executor.utilities.sql import query_df_with_type_infer_fallback
from mindsdb.api.executor.exceptions import NotSupportedYet

from .base import BaseStepCall


class JoinStepCall(BaseStepCall):

    bind = JoinStep

    def call(self, step):
        left_data = self.steps_data[step.left.step_num]
        right_data = self.steps_data[step.right.step_num]

        if right_data.is_prediction or left_data.is_prediction:
            # ignore join condition, use row_id
            l_row_ids = left_data.find_columns('__mindsdb_row_id')
            r_row_ids = right_data.find_columns('__mindsdb_row_id')

            if len(l_row_ids) == 0:
                if len(r_row_ids) == 0:
                    raise RuntimeError('Unable to find row id')
                else:
                    # copy from right to left
                    idx = right_data.get_col_index(r_row_ids[0])
                    left_data.set_column_values('__mindsdb_row_id', right_data.get_column_values(idx))
                    l_row_ids = left_data.find_columns('__mindsdb_row_id')
            elif len(r_row_ids) == 0:
                # copy from left to right
                idx = left_data.get_col_index(l_row_ids[0])
                right_data.set_column_values('__mindsdb_row_id', left_data.get_column_values(idx))
                r_row_ids = right_data.find_columns('__mindsdb_row_id')

            a_row_id = l_row_ids[0].get_hash_name(prefix='A')
            b_row_id = r_row_ids[0].get_hash_name(prefix='B')

            join_condition = f'table_a.{a_row_id} = table_b.{b_row_id}'

            join_type = step.query.join_type.lower()
            if join_type == 'join':
                # join type is not specified. using join to prediction data
                if left_data.is_prediction:
                    join_type = 'left join'
                elif right_data.is_prediction:
                    join_type = 'right join'
            table_a, names_a = left_data.to_df_cols(prefix='A')
            table_b, names_b = right_data.to_df_cols(prefix='B')

            query = f"""
                SELECT * FROM table_a {join_type} table_b
                ON {join_condition}
            """
            resp_df, _description = query_df_with_type_infer_fallback(query, {
                'table_a': table_a,
                'table_b': table_b
            })
        else:
            # Register DataFrames with DuckDB using the original table aliases
            # so DuckDB resolves column references in ON conditions natively,
            # including functions like LOWER(), SPLIT_PART(), etc.
            left_alias = left_data.columns[0].table_alias if left_data.columns else 'table_a'
            right_alias = right_data.columns[0].table_alias if right_data.columns else 'table_b'
            if left_alias == right_alias:
                right_alias = f'{right_alias}_r'

            left_df = left_data.to_df()
            right_df = right_data.to_df()

            # Build SELECT with hash-named aliases to avoid column name collisions
            names_a = {}
            select_parts = []
            for col in left_data.columns:
                hash_name = col.get_hash_name('A')
                names_a[hash_name] = col
                select_parts.append(f'"{left_alias}"."{col.alias}" AS "{hash_name}"')

            names_b = {}
            for col in right_data.columns:
                hash_name = col.get_hash_name('B')
                names_b[hash_name] = col
                select_parts.append(f'"{right_alias}"."{col.alias}" AS "{hash_name}"')

            if step.query.condition is None:
                if len(left_data) * len(right_data) < 10 ** 7:
                    step.query.condition = BinaryOperation(op='=', args=[Constant(0), Constant(0)])
                else:
                    raise NotSupportedYet(
                        'Unable to join tables without a condition: the resulting cross join '
                        f'would produce {len(left_data) * len(right_data):,} rows '
                        f'({len(left_data):,} x {len(right_data):,}), exceeding the 10,000,000 row limit.\n'
                        'Hint: Add an ON clause, e.g.: SELECT * FROM t1 JOIN t2 ON t1.id = t2.id'
                    )

            condition = copy.deepcopy(step.query.condition)
            join_condition = SqlalchemyRender('postgres').get_string(condition)
            join_type = step.query.join_type

            select_clause = ', '.join(select_parts)
            query = f"""
                SELECT {select_clause}
                FROM "{left_alias}" {join_type} "{right_alias}"
                ON {join_condition}
            """
            resp_df, _description = query_df_with_type_infer_fallback(query, {
                left_alias: left_df,
                right_alias: right_df,
            })

        resp_df.replace({np.nan: None}, inplace=True)

        names_a.update(names_b)
        data = ResultSet.from_df_cols(df=resp_df, columns_dict=names_a, strict=False)

        for col in data.find_columns('__mindsdb_row_id'):
            data.del_column(col)

        return data
