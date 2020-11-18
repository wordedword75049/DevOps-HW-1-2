import psycopg2
import pandas as pd
import numpy as np
import re


pattern = r'NCT\d{8}'  # pattern for NCT - id


def to_string(records: list, _id, last_line_flag: int) -> str:
    """
    This function turns a list of tuples
    which was received from database query
    to string which we put in a spreadsheet cell

    Args:
    records - list of tuples
    example:
    [('graft-versus-tumor induction therapy',),
    ('graft-versus-tumor induction therapy',),
    ('Induction therapy (Regimen 1, Course 1, Cycle A1',)]
    _id - either id if we want to specify result for id or None
    last_line_flag - either 0 or 1

    Return:
    string
    example:
    'graft-versus-tumor induction therapy;
     graft-versus-tumor induction therapy;
     Induction therapy (Regimen 1, Course 1, Cycle A1'
    (as a single line)
    """
    if not records:
        return ''
    string = f'{_id}: ' if _id else ''
    string += '; '.join([str(record[0]) for record in records])
    if not last_line_flag:
        string += '\n'
    return string


def count_entry(main_word: str, string: str) -> int:
    """
    Counts how many times a single word
    is found in a sentence
    """
    count = 0
    if not string:
        return count
    for word in string.split():
        if main_word in word.lower():
            count += 1
    return count


def connect_to_database():
    conn_string = "host='k8s-svc-bridge.devbg.us' port = '31575' dbname='aact' user='reader' password='aBoDOaVaTi'"
    print("Connecting to database\n->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print("Connected!\n")
    return conn, cursor


def close_connection():
    cursor.close()
    conn.close()


def nct_interventions(_id: str, cursor) -> list:
    """
    Performs a sql query. Returns list of
    tuples as in 'to_string' function example
    """
    request_string = f"SELECT name FROM interventions WHERE nct_id = '{_id}'"
    cursor.execute(request_string)
    records = cursor.fetchall()
    return records


def ash_interventions(candidate, cursor):
    """
    Performs a sql query. Returns list of
    tuples as in 'to_string' function example
    """
    candidate = candidate.lower()
    candidate = '\'' + candidate + '\''
    intervention_types = ('Drug', 'Biological', 'Combination Product', 'Radiation', 'Genetic')
    request_string = \
        f"""SELECT i.name, i.nct_id FROM ctgov.interventions i
                    WHERE (i.intervention_type IN {intervention_types}) AND
                            ((POSITION({candidate} in LOWER(i.name)) > 0))
                    ORDER BY  i.nct_id
                    LIMIT 5"""
    cursor.execute(request_string)
    records = cursor.fetchall()
    return records


def ash_interventions_df(df: pd.DataFrame) -> list:
    """
    For given dataframe this function takes every
    candidate and performs ash_query for it.
    Received records are turned to string and
    it is counted how many times a candidate
    is found in that string.
    Results are put into 2 lists
    """
    df_interventions, df_interventions_count = [], []
    for i in range(len(df)):
        last_line_flag = 0
        candidate = df.candidate[i]
        cand_int = ''
        if pd.isna(candidate):
            df_interventions.append('invalid candidate')
            df_interventions_count.append('invalid candidate')
            continue
        records = ash_interventions(candidate, cursor)
        for record in records:
            if records.index(record) == len(records) - 1:
                last_line_flag = 1
            cand_int += to_string([record], record[1], last_line_flag)
        df_interventions.append(cand_int)
        df_interventions_count.append(count_entry(candidate, cand_int))
    return df_interventions, df_interventions_count


def nct_interventions_df(df: pd.DataFrame) -> list:
    """
    For given dataframe this function takes each
    candidate/id and performs nct_query for it.
    Received records are turned to string and
    it is counted how many times a candidate
    is found in that string.
    Results are put into 2 lists
    """
    df_interventions, df_interventions_count = [], []
    id_titles = ('_ids', 'nct_ids', 'id', 'ids')
    id_col_name = [col for col in df.columns if col in id_titles]
    for i in range(len(df)):
        last_line_flag = 0
        candidate = df.candidate[i]
        cand_int = ''
        if candidate is np.nan:
            df_interventions.append('invalid candidate')
            df_interventions_count.append('invalid candidate')
            continue
        if not pd.isna(df[f'{id_col_name[0]}'][i]):
            ids = re.findall(pattern, df[f'{id_col_name[0]}'][i])
        else:
            df_interventions.append('invalid id')
            df_interventions_count.append('invalid id')
            continue
        for _id in ids:
            if ids.index(_id) == len(ids) - 1:
                last_line_flag = 1
            records = nct_interventions(_id, cursor)
            int_str = to_string(records, _id, last_line_flag)
            cand_int += int_str
        df_interventions.append(cand_int)
        df_interventions_count.append(count_entry(candidate, cand_int))
        # if :  ADD for intervention_other_names
    return df_interventions, df_interventions_count


conn, cursor = connect_to_database()


def execute_interventions(csv_file: str, flag: int) -> list:
    df = pd.read_csv(csv_file)
    if flag == 0:
        df_interventions, df_interventions_count = ash_interventions_df(df)
    elif flag == 1:
        df_interventions, df_interventions_count = nct_interventions_df(df)
    else:
        print('incorrect flag')
        return None
    return df_interventions, df_interventions_count


#close_connection()
