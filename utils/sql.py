import sqlparse
from collections import OrderedDict


def sql_pred_parse(pred):
    # parse sql results and fix general errors

    pred = f" * FROM{pred}"

    # fix for no states
    if pred == " * FROM  WHERE ":
        return {}

    # Here we need to write a parser to convert back to dialogue state
    pred_slot_values = []
    # pred = pred.lower()
    parsed = sqlparse.parse(pred)
    if not parsed:
        return {}
    stmt = parsed[0]
    sql_toks = pred.split()
    operators = [" = ", " LIKE ", " < ", " > ", " >= ", " <= "]

    if "AS" in pred:
        as_indices = [i for i, x in enumerate(sql_toks) if x == "AS"]

        table_name_map_dict = {
            sql_toks[indice + 1].replace(",", ""): sql_toks[indice - 1]
            for indice in as_indices
        }
        slot_values_str = str(stmt.tokens[-1]).replace("_", " ").replace("""'""", "").replace("WHERE ", "")
        for operator in operators:
            slot_values_str = slot_values_str.replace(operator, "-")
        slot_values = slot_values_str.split(" AND ")

        for sv in slot_values:
            for t_ in table_name_map_dict:
                sv = sv.replace(f"{t_}.", f"{table_name_map_dict[t_]}-")
            pred_slot_values.append(sv)
    else:

        table_name = sql_toks[sql_toks.index("FROM") + 1]

        slot_values_str = str(stmt.tokens[-1]).replace("_", " ").replace("""'""", "").replace("WHERE ", "")
        for operator in operators:
            slot_values_str = slot_values_str.replace(operator, "-")
        slot_values = slot_values_str.split(" AND ")

        pred_slot_values.extend(
            [f"{table_name}-{sv}" for sv in slot_values if slot_values != ['']]
        )

    pred_slot_values = {'-'.join(sv_pair.split('-')[:-1]): sv_pair.split('-')[-1] for sv_pair in pred_slot_values}

    # remove _ in SQL columns
    pred_slot_values = {slot.replace('_', ' '): value for slot, value in pred_slot_values.items()}

    # fix typos
    # pred_slot_values, _ = typo_fix(pred_slot_values)

    return pred_slot_values


def sv_dict_to_string(svs, sep=' ', sort=True):
    result_list = [f"{s.replace('-', sep)}{sep}{v}" for s, v in svs.items()]
    if sort:
        result_list = sorted(result_list)
    return ', '.join(result_list)


def slot_values_to_seq_sql(original_slot_values, single_answer=False):
    sql_str = ""
    tables = OrderedDict()
    col_value = {}

    # add '_' in SQL columns
    slot_values = {}
    for slot, value in original_slot_values.items():
        if ' ' in slot:
            slot = slot.replace(' ', '_')
        slot_values[slot] = value

    for slot, value in slot_values.items():
        assert len(slot.split("-")) == 2

        if '|' in value:
            value = value.split('|')[0]

        table, col = slot.split("-")  # slot -> table-col

        if table not in tables.keys():
            tables[table] = []
        tables[table].append(col)

        # sometimes the answer is ambiguous
        if single_answer:
            value = value.split('|')[0]
        col_value[slot] = value

    where_clause = []
    # When there is only one table
    if len(tables.keys()) == 1:
        table = list(tables.keys())[0]
        where_clause.extend(
            f'{col} = {col_value[f"{table}-{col}"]}' for col in tables[table]
        )
        return f'SELECT * FROM {table} WHERE {" AND ".join(where_clause)}'
    else:
        # We observed that Codex has variety in the table short names, here we just use a simple version.
        from_clause = []
        for i, table in enumerate(tables.keys()):
            t_i = f"t{i + 1}"
            from_clause.append(f"{table} AS {t_i}")
            where_clause.extend(
                f'{t_i}.{col} = {col_value[f"{table}-{col}"]}'
                for col in tables[table]
            )
        return f'SELECT * FROM {", ".join(from_clause)} WHERE {" AND ".join(where_clause)}'
