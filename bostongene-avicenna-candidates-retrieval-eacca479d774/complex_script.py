import argparse
import os
import sys

import rdflib
import re
import nltk
import psycopg2
import datetime

nltk.download('stopwords')

from phase_taker import work_on_single_file
from found_in_avicenna import execute_found_in_avicenna
from interventions import execute_interventions
from names_by_cancer_gov import execute_cancer_gov_names
from fda_labels import work_fda_labels
import csv
from enum import Enum


class Flag(Enum):
    No_flag = -1
    TP = 1
    FP = 2
    black_list = 3


# directory = './experiments/phase_recognition/candidates_from_2019_10_24_to_2019_10_30_checked.csv
conn_string = "host='localhost' dbname='drug-annotations' user='postgres' password='12345'"
test_file = './experiments/candidates_from_2019_09_19_to_2019_09_26_checked.csv'


def get_last_id(cursor, id_type, table):
    cursor.execute(f"""SELECT {id_type} FROM {table}""")
    rows = cursor.fetchall()
    last_id = 0
    if rows != []:
        last_id = rows[-1][0]
    return last_id


def get_all_items_db(cursor, column, table):
    cursor.execute(f"""SELECT {column} FROM {table}""")
    rows = cursor.fetchall()
    return rows


def get_information_by_nct(can_id, can_name, nct):
    conn_string = "host='k8s-svc-bridge.devbg.us' port = '31575' dbname='aact' user='reader' password='aBoDOaVaTi'"
    conn = psycopg2.connect(conn_string)
    curs = conn.cursor()
    curs.execute(f"""SELECT s.nct_id,
                            s.phase,
                             i.name,
                             s.brief_title
                        FROM ctgov.studies s
                    LEFT JOIN ctgov.brief_summaries bs ON s.nct_id = bs.nct_id
                    LEFT JOIN ctgov.design_groups dg ON s.nct_id = dg.nct_id
                    LEFT JOIN ctgov.design_group_interventions dgi ON dg.id = dgi.design_group_id
                    LEFT JOIN ctgov.interventions i ON dgi.intervention_id = i.id
                    LEFT JOIN ctgov.intervention_other_names ion ON i.id = ion.intervention_id
                    WHERE s.nct_id = '{nct}'""")
    record = curs.fetchone()
    curs.close()
    conn.close()
    # print(record)
    if record != None:
        return [can_id, can_name, nct, record[1], record[3], record[2]]
    else:
        return []


def stringify_list(item):
    result_string = ''
    if len(item) > 1:
        for part in item:
            result_string = result_string + part + ' '
    else:
        result_string = item[0]
    return result_string


def get_index_positions(list_of_elems, element):
    ''' Returns the indexes of all occurrences of give element in
    the list- listOfElements '''
    index_pos_list = []
    index_pos = 0
    while True:
        try:
            # Search for item in list from indexPos to the end of list
            index_pos = list_of_elems.index(element, index_pos)
            # Add the index position in list
            index_pos_list.append(index_pos)
            index_pos += 1
        except ValueError as e:
            break
    return index_pos_list


def nct_periodize(file):
    clear_filename = file.split('/')[-1].split('.')[-2].split('_')
    from_index = clear_filename.index('from')
    to_index = clear_filename.index('to')
    from_date = str(clear_filename[from_index + 3]) + '.' + str(clear_filename[from_index + 2]) + '.' + str(
        clear_filename[from_index + 1])
    to_date = str(clear_filename[to_index + 3]) + '.' + str(clear_filename[to_index + 2]) + '.' + str(
        clear_filename[to_index + 1])
    return [from_date, to_date]


def deapostrofy(line):
    if line is not None:
        while '\'' in line:
            ind = line.index('\'')
            line = line[:ind] + '`' + line[ind + 1:]
    return line


def deThesaurusify(file, res_name):
    index = 0
    g = rdflib.Graph()
    print('started deThesaurusifying')
    Source = open(file, encoding="utf-8")
    Source_Txt = ''
    for line in Source.readlines():
        Source_Txt += line

    g.parse(data=Source_Txt, format="application/rdf+xml")

    FILE = open(res_name + '.txt', 'w')

    for s, p, o in g:
        index = index + 1
        if index % 1000 == 0:
            print(index)
        p = re.sub("\S*#", "", p)
        s = re.sub("\S*#", "", s)
        o = re.sub("\S*#", "", o)
        FILE.write(str(s) + "  " + str(p) + "  " + str(o) + "\n")
    print('finished deThesaurusifying')


def get_thesaurus_lines(file_path):
    lines_array = []
    sub_array = []
    pred_array = []
    obj_array = []
    index = 0
    print(file_path)
    with open(file_path, 'r') as f:
        print('file opened')
        while True:
            index = index + 1
            if index % 500000 == 0:
                print('read ' + str(index) + ' lines')
            line = f.readline()
            if line:
                split_line = line[:-1].split('  ')
                lines_array.append(split_line)
                sub_array.append(split_line[0])
                pred_array.append(split_line[1])
                obj_array.append(split_line[2])
            else:
                break
    print(len(lines_array))
    return lines_array, sub_array, pred_array, obj_array


lines_from_file, array_of_subjects, array_of_predicates, array_of_objects = get_thesaurus_lines('./text_thesaurus.txt')


def find_synonyms_for_code(code):
    indexes = get_index_positions(array_of_objects, code)
    synonym_data = []
    for i in indexes:
        if lines_from_file[i][1] == 'annotatedSource':
            indexes_of_addition = get_index_positions(array_of_subjects, lines_from_file[i][0])
            addition_sub = [array_of_subjects[i] for i in indexes_of_addition]
            addition_pred = [array_of_predicates[i] for i in indexes_of_addition]
            addition_obj = [array_of_objects[i] for i in indexes_of_addition]
            if ('annotatedProperty' in addition_pred) & (
                    addition_obj[addition_pred.index('annotatedProperty')] == 'P90'):
                synonym_data.append(
                    [addition_obj[addition_pred.index('annotatedTarget')], addition_obj[addition_pred.index('P383')],
                     addition_obj[addition_pred.index('P384')]])
    # for each_syn in synonym_data:
    # print(each_syn)
    return synonym_data


def nci_full_research(candidate_name):
    result_list = []
    found_codes = []
    canonicals_not_cleared = [s for s in lines_from_file if (candidate_name.lower() in s[2].lower()) & (s[1] == 'P108')]
    # print(canonicals_not_cleared)
    for each_string in canonicals_not_cleared:
        result_list.append([each_string[0], each_string[2], find_synonyms_for_code(each_string[0])])
    #for result in result_list:
    #    print(result)
    #    print('\n')
    return result_list


# nci_full_research('azad')


def destring_ncts(line):
    res_list = []
    if len(line.split(',')) > 1:
        for each_nct in line.split(','):
            if each_nct[0] == '{':
                res_list.append(each_nct[2:-1])
            elif each_nct[-1] == '}':
                res_list.append(each_nct[2:-2])
            else:
                res_list.append(each_nct[2:-1])
    else:
        res_list.append(line[2:-2])
    return res_list


def insert_candidate(cursor, insert_info):
    insert_request = f"""INSERT INTO candidates VALUES ({insert_info[0]}, '{insert_info[1]}', '{insert_info[2]}', '{insert_info[3]}', '{insert_info[4]}', '{insert_info[5]}', '{insert_info[6]}')"""
    cursor.execute(insert_request)
    cursor.execute("COMMIT")


def insert_clinicaltrials(cursor, insert_info):
    used_ncts = get_all_items_db(cursor, 'nct_id', 'clinicaltrials_information')
    for each_insert in insert_info:
        # print(each_insert)
        if each_insert != []:
            if tuple((each_insert[2],)) not in used_ncts:
                insert_request = f"""INSERT INTO clinicaltrials_information VALUES ({each_insert[0]}, '{each_insert[1]}', '{each_insert[2]}', '{each_insert[3]}', '{deapostrofy(each_insert[4])}', '{deapostrofy(each_insert[5])}')"""
                cursor.execute(insert_request)
                used_ncts.append(tuple((each_insert[2],)))
            else:
                pass
                # print('this nct is already there')
    cursor.execute("COMMIT")


def insert_nct_batch(cursor, insert_info):
    insert_request = f"""INSERT INTO nct_batch VALUES ({insert_info[0]}, 'NCT batch {insert_info[0]} from {insert_info[1]} to {insert_info[2]}', '{str(datetime.datetime.now())}' ,'{insert_info[1]}', '{insert_info[2]}')"""
    cursor.execute(insert_request)
    cursor.execute("COMMIT")


def insert_abstracts(cursor, insert_info):
    insert_request = f"""INSERT INTO abstracts VALUES ({insert_info[0]}, '{insert_info[1]}', '{insert_info[2]}', '{insert_info[3]}')"""
    cursor.execute(insert_request)
    cursor.execute("COMMIT")


def insert_nct_sources(cursor, insert_info):
    insert_request = f"""INSERT INTO nct_sources VALUES ({insert_info[0]}, {insert_info[1]}, '{insert_info[2]}', '{insert_info[3]}', '{insert_info[4]}', {insert_info[5]})"""
    cursor.execute(insert_request)
    cursor.execute("COMMIT")


def insert_abstract_sources(cursor, insert_info):
    insert_request = f"""INSERT INTO abstract_sources VALUES ({insert_info[0]}, {insert_info[1]}, '{insert_info[2]}', '{insert_info[3]}', '{insert_info[4]}', {insert_info[5]})"""
    cursor.execute(insert_request)
    cursor.execute("COMMIT")


def insert_nci_codes(cursor, insert_info):
    if insert_info != []:
        separate_codes = [s[0] for s in insert_info]
        separate_names = [s[1] for s in insert_info]
        used_codes = get_all_items_db(cursor, 'nci_thesaurus_code', 'nci_codes')
        for code, name in zip(separate_codes, separate_names):
            if tuple((code,)) not in used_codes:
                # print('there is no ' + code + ' in')
                # print(used_codes)
                insert_request = f"""INSERT INTO nci_codes VALUES ('{code}', '{deapostrofy(name)}')"""
                cursor.execute(insert_request)
                used_codes.append(tuple((code,)))
            else:
                pass
                # print('found '+code+' in')
                # print(used_codes)
    cursor.execute("COMMIT")


def insert_nci_info(cursor, insert_info):
    last_id = get_last_id(cursor, 'art_id', 'nci_information')
    used_codes = get_all_items_db(cursor, 'nci_thesaurus_code', 'nci_information')
    if insert_info[2] != []:
        codes = [s[0] for s in insert_info[2]]
        for each_code in codes:
            if tuple((each_code,)) not in used_codes:
                last_id = last_id + 1
                insert_request = f"""INSERT INTO nci_information VALUES ({last_id}, {insert_info[0]}, '{insert_info[1]}', '{each_code}')"""
                cursor.execute(insert_request)
                used_codes.append(tuple((each_code,)))
    cursor.execute("COMMIT")


def insert_nci_synonyms(cursor, insert_info):
    last_id = get_last_id(cursor, 'id', 'nci_synonyms')
    used_codes = get_all_items_db(cursor, 'nci_thesaurus_code', 'nci_synonyms')
    if insert_info != []:
        used_syns = []
        for each_info in insert_info:
            if tuple((each_info[0],)) not in used_codes:
                for each_syn in each_info[2]:
                    if deapostrofy(each_syn[0]).lower() not in used_syns:
                        if each_syn[1] in ['PT', 'DN', 'BR', 'FB']:
                            last_id = last_id + 1
                            insert_request = f"""INSERT INTO nci_synonyms VALUES ({last_id}, '{each_info[0]}', '{deapostrofy(each_syn[0])}', '{each_syn[2]}', '{each_syn[1]}')"""
                            cursor.execute(insert_request)
                            used_syns.append(deapostrofy(each_syn[0]).lower())
                used_codes.append(tuple((each_info[0],)))
    cursor.execute("COMMIT")


def find_code(string_t, d_list):
    ret = None
    print(d_list)
    print(type(d_list))
    print(len(d_list))
    for each in d_list:
        print(each)
        code_try = each.get(string_t)
        if code_try is not None:
            ret = code_try
            break
    return ret


def insert_fda_information(cursor, insert_info):
    last_id = get_last_id(cursor, 'art_id', 'fda_information')
    used_drugs = []
    for each_canonical in insert_info[0]:
        for term_with_info in each_canonical.items():
            code = find_code(term_with_info[0], insert_info[1])
            for each_syn in term_with_info[1].items():
                name = each_syn[0]
                for each_app in each_syn[1].items():
                    if [name, each_app[0], each_app[1][1]] not in used_drugs:
                        last_id = last_id + 1
                        insert_request = f"""INSERT INTO fda_information VALUES ({last_id}, '{name}', '{code}', '{each_app[0]}', '{each_app[1][1]}', '{each_app[1][0]}')"""
                        used_drugs.append([name, each_app[0], each_app[1][1]])
                        cursor.execute(insert_request)
                        cursor.execute("COMMIT")


def merge_brief_interv(can_id, name, briefs, interventions):
    result_list = []
    used_ncts = []
    for each_brief in briefs:
        result_list.append(get_information_by_nct(can_id, name, each_brief[1]))
        used_ncts.append(each_brief[1])
    if interventions != '':
        int_prepared_data = []
        int_list = interventions.split('\n')
        for intervention in int_list:
            parts = intervention.split(':')
            int_prepared_data.append(parts)
        for each_int in int_prepared_data:
            if each_int[0] not in used_ncts:
                result_list.append(get_information_by_nct(can_id, name, each_int[0]))
                used_ncts.append(each_int[0])
    return result_list


def flag_check(candidate_str, tp, fp, black_list):
    flag = Flag.No_flag
    if candidate_str[tp] != '':
        flag = Flag.TP
    if (fp != -1) & (candidate_str[fp] != ''):
        flag = Flag.FP
    if candidate_str[black_list] != '':
        flag = Flag.black_list

    # print('found flag ' + flag.name)
    return flag


def acquire_indexes(title_str):
    tp = title_str.index('TP')
    ci = title_str.index('candidate')
    try:
        ii1 = title_str.index('_ids')
    except ValueError:
        ii1 = -1
    try:
        ii2 = title_str.index('nct_ids')
    except ValueError:
        ii2 = -1
    ii = max(ii1, ii2)
    try:
        fp = title_str.index('FP')
    except ValueError:
        fp = -1
    try:
        si = title_str.index('sents')
    except ValueError:
        si = -1
    black_list = title_str.index('Add_to_black_list')
    return tp, fp, black_list, ci, si, ii


def prepare_for_fda(data_list, filename):
    flag_list = []
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, dialect='excel')
        title_str = next(spamreader)

        try:
            ti1 = title_str.index('TP')
        except ValueError:
            ti1 = -1
        try:
            ti2 = title_str.index('Therapy')
        except ValueError:
            ti2 = -1
        tp_index = max(ti1, ti2)

        black_list_index = title_str.index('Add_to_black_list')

        for row in spamreader:
            flag_list.append(flag_check(row, tp_index, -1, black_list_index))

    result_list = []
    codes_list = []
    for each_candidate, flag in zip(data_list, flag_list):
        if flag != Flag.black_list:
            if len(each_candidate) > 3:
                can_dict = {}
                codes_dict = {}
                candidate_info = nci_full_research(each_candidate)
                for each_found in candidate_info:
                    synonym_list = []
                    codes_dict.update([(each_found[1].lower(), each_found[0])])
                    for each_synonym in each_found[2]:
                        if each_synonym[0].lower() != each_found[1].lower():
                            if each_synonym[0].lower() not in synonym_list:
                                if each_synonym[1] in ['PT', 'DN', 'BR', 'FB']:
                                    synonym_list.append(each_synonym[0].lower())
                    can_dict.update([(each_found[1].lower(), synonym_list)])
                if can_dict != {}:
                    result_list.append(can_dict)
                    codes_list.append(codes_dict)

        print('finished with ' + each_candidate)
    return result_list, codes_list


def acquire_abstract_data_from_file(cursor, filename):
    last_candidate_id = get_last_id(cursor, 'id', 'candidates')
    candidates = []
    nct_sources = []
    source_batch_info = []
    source_info = []
    last_abst_id = get_last_id(cursor, 'art_id', 'abstract_sources')
    cur_batch_id = get_last_id(cursor, 'astract_id', 'abstracts') + 1

    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, dialect='excel')
        title_str = next(spamreader)

        try:
            ti1 = title_str.index('TP')
        except ValueError:
            ti1 = -1
        try:
            ti2 = title_str.index('Therapy')
        except ValueError:
            ti2 = -1
        tp_index = max(ti1, ti2)

        candidate_index = title_str.index('candidate')

        try:
            ii1 = title_str.index('_ids')
        except ValueError:
            ii1 = -1
        try:
            ii2 = title_str.index('ids')
        except ValueError:
            ii2 = -1
        ids_index = max(ii1, ii2)

        try:
            sent_index = title_str.index('sents')
        except ValueError:
            sent_index = -1

        black_list_index = title_str.index('Add_to_black_list')

        for row in spamreader:
            # print(row)
            candidate_info = []
            last_candidate_id = last_candidate_id + 1
            last_abst_id = last_abst_id + 1
            found_sent = []
            absts_in_file = destring_ncts(row[ids_index])
            if sent_index == -1:
                for each_id in absts_in_file:
                    found_sent.append('-')
            else:
                found_sent = destring_ncts(row[sent_index])
            # print(ncts_in_file)
            # print(found_sent)
            source_info.append(
                [last_abst_id, last_candidate_id, row[candidate_index], found_sent, absts_in_file, cur_batch_id])
            # candidates table info
            candidate_info.append(last_candidate_id)
            candidate_info.append(flag_check(row, tp_index, -1, black_list_index))
            # sources_info(depending on source type)
            candidates.append(candidate_info)
            # print(candidate_info)
    return candidates, cur_batch_id, source_info


def acquire_data_from_file(cursor, filename):
    last_candidate_id = get_last_id(cursor, 'id', 'candidates')
    candidates = []
    nct_sources = []
    source_batch_info = []
    source_info = []
    last_nct_id = get_last_id(cursor, 'art_id', 'nct_sources')
    cur_batch_id = get_last_id(cursor, 'nct_batch_id', 'nct_batch') + 1
    if (filename.find('aacr') == -1) & (filename.find('ASH') == -1):
        source_type = 'nct'
    else:
        source_type = 'absracts'
    if source_type == 'nct':
        from_and_to = nct_periodize(filename)
        source_batch_info = [cur_batch_id, from_and_to[0], from_and_to[1]]
    print(source_type)
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, dialect='excel')
        titles = next(spamreader)
        tp_index, fp_index, black_list_index, candidate_index, sent_index, ids_index = acquire_indexes(titles)
        for row in spamreader:
            # print(row)
            candidate_info = []
            last_candidate_id = last_candidate_id + 1
            last_nct_id = last_nct_id + 1
            found_sent = []
            ncts_in_file = destring_ncts(row[ids_index])
            if sent_index == -1:
                for each_id in ncts_in_file:
                    found_sent.append('-')
            else:
                found_sent = row[sent_index]
            # print(ncts_in_file)
            # print(found_sent)
            source_info.append(
                [last_nct_id, last_candidate_id, row[candidate_index], found_sent, ncts_in_file, cur_batch_id])
            # candidates table info
            candidate_info.append(last_candidate_id)
            candidate_info.append(flag_check(row, tp_index, fp_index, black_list_index))
            # sources_info(depending on source type)
            candidates.append(candidate_info)
            # print(candidate_info)
    return candidates, source_batch_info, source_info


def main_db_actions(cursor):
    rows = []
    # Выполняем запрос.
    cursor.execute("DROP TABLE IF EXISTS candidates CASCADE")
    cursor.execute("DROP TABLE IF EXISTS nci_information  CASCADE")
    cursor.execute("DROP TABLE IF EXISTS nct_batch CASCADE")
    cursor.execute("DROP TABLE IF EXISTS nci_codes CASCADE")
    cursor.execute("DROP TABLE IF EXISTS fda_information")
    cursor.execute("DROP TABLE IF EXISTS clinicaltrials_information")
    cursor.execute("DROP TABLE IF EXISTS nct_sources")
    cursor.execute("DROP TABLE IF EXISTS nci_synonyms")
    cursor.execute("DROP TABLE IF EXISTS abstract_sources")
    cursor.execute("DROP TABLE IF EXISTS abstracts")
    cursor.execute(
        "CREATE TABLE candidates ( id INTEGER, candidate_name VARCHAR, flag VARCHAR, max_phase_aact VARCHAR, max_phase_nct VARCHAR, found_in_avicenna VARCHAR, count_in_avicenna VARCHAR, CONSTRAINT candidate_pk PRIMARY KEY (id, candidate_name));")
    cursor.execute(
        "CREATE TABLE nci_codes (nci_thesaurus_code VARCHAR UNIQUE, canonical_name VARCHAR, PRIMARY KEY (nci_thesaurus_code));")
    cursor.execute(
        "CREATE TABLE nci_information (art_id INTEGER, id INTEGER, candidate_name VARCHAR, nci_thesaurus_code VARCHAR, CONSTRAINT nci_pk PRIMARY KEY (art_id, nci_thesaurus_code), CONSTRAINT nci_fk FOREIGN KEY (id, candidate_name) REFERENCES candidates(id, candidate_name), FOREIGN KEY (nci_thesaurus_code) REFERENCES nci_codes(nci_thesaurus_code));")
    cursor.execute(
        "CREATE TABLE fda_information ( art_id integer, drug_name VARCHAR, nci_thesaurus_code VARCHAR, drug_application VARCHAR, fda_label_date VARCHAR, fda_label_link VARCHAR, PRIMARY KEY (art_id), FOREIGN KEY (nci_thesaurus_code) REFERENCES nci_codes(nci_thesaurus_code));")
    cursor.execute(
        "CREATE TABLE clinicaltrials_information (id INTEGER, candidate_name VARCHAR, nct_id VARCHAR, nct_phase VARCHAR, brief_title VARCHAR, interventions_w_candidate VARCHAR, CONSTRAINT clinical_pk PRIMARY KEY (id, brief_title), CONSTRAINT clinical_fk FOREIGN KEY (id, candidate_name) REFERENCES candidates(id, candidate_name));")
    cursor.execute(
        "CREATE TABLE nct_batch (nct_batch_id INTEGER, label VARCHAR, batch_creation_date VARCHAR, batch_period_start VARCHAR, batch_period_end VARCHAR, PRIMARY KEY (nct_batch_id));")
    cursor.execute(
        f"""INSERT INTO nct_batch VALUES (-1, 'Blacklist Migration Batch', '{str(datetime.datetime.now())}', 'very long ago', 'very long ago');""")
    cursor.execute(
        "CREATE TABLE nct_sources (art_id INTEGER, id INTEGER, candidate_name VARCHAR, sentences VARCHAR, nct_id VARCHAR, nct_batch_id INTEGER, CONSTRAINT nct_pk PRIMARY KEY (art_id, nct_id), CONSTRAINT nct_fk FOREIGN KEY (id, candidate_name) REFERENCES candidates(id, candidate_name), FOREIGN KEY (nct_batch_id) REFERENCES nct_batch(nct_batch_id));")
    cursor.execute(
        "CREATE TABLE nci_synonyms (id INTEGER, nci_thesaurus_code VARCHAR, term VARCHAR, source VARCHAR, type VARCHAR, PRIMARY KEY (id), FOREIGN KEY (nci_thesaurus_code) REFERENCES nci_codes(nci_thesaurus_code));")
    cursor.execute(
        "CREATE TABLE abstracts (abstract_id VARCHAR, conference_name VARCHAR, conference_date VARCHAR, conference_link VARCHAR, PRIMARY KEY (abstract_id));")
    cursor.execute(
        "CREATE TABLE abstract_sources (art_id INTEGER, id INTEGER, candidate_name VARCHAR, sentences VARCHAR, abstract VARCHAR, abstract_id VARCHAR, CONSTRAINT abstracts_pk PRIMARY KEY (art_id, id), CONSTRAINT abstracts_fk FOREIGN KEY (id, candidate_name) REFERENCES candidates(id, candidate_name), FOREIGN KEY (abstract_id) REFERENCES abstracts(abstract_id));")
    cursor.execute("COMMIT")
    # cursor.execute("INSERT INTO candidates VALUES (1, 'can_name', 'flag', 'phase 4', 'nct')")
    # cursor.execute("INSERT INTO avicenna_information VALUES (1, 'can_name', 'found 1', 'count 1')")
    # cursor.execute("SELECT * FROM candidates can LEFT JOIN avicenna_information ainf on can.id = ainf.id")
    # rows = cursor.fetchall()
    # for row in rows:
    # print(row)


def parse(argv):  # Command line interface - not used
    parser = argparse.ArgumentParser(description="""
     Argument parser for table updater
        """, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('Name', action='store', type=str)
    parser.add_argument('Date', action='store', type=str)
    parser.add_argument('Link', action='store', type=str)

    return parser.parse_args(argv[1:])


def work_with_candidates_database(file, can_script_result, av_script_result, interv_script_result, canonicals_and_codes,
                                  fda_info, names_and_codes):
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    cursor = conn.cursor()
    #main_db_actions(cursor)
    candidates_info = []
    if (file.find('aacr') == -1) & (file.find('ASH') == -1):
        source_type = 'nct'
    else:
        source_type = 'abstracts'
    if source_type == 'nct':
        id_and_flag, batch_info, source_info = acquire_data_from_file(cursor, file)
        insert_nct_batch(cursor, batch_info)
    else:
        id_and_flag, batch_info, source_info = acquire_abstract_data_from_file(cursor, file)
        args = parse(sys.argv)
        insert_abstracts(cursor, [batch_info, args.Name, args.Date, args.Link])
    for service, can_base_info, avicenna_found, avicenna_count, interv_info, source_data, canonicals, codes in zip(
            id_and_flag, can_script_result, av_script_result[0], av_script_result[1], interv_script_result, source_info,
            canonicals_and_codes[0], canonicals_and_codes[1]):
        if service[1] != Flag.black_list:
            nct_check = can_base_info[2].split('\"')
            if len(nct_check) > 1:
                nct_to_add = nct_check[3]
            else:
                nct_to_add = nct_check[0]
            insert_candidate(cursor,
                             [service[0], can_base_info[0], service[1], can_base_info[1], nct_to_add, avicenna_found,
                              avicenna_count])
            if len(can_base_info[0]) > 3:
                thesaurus_canidate_info = nci_full_research(can_base_info[0])
                insert_nci_codes(cursor, thesaurus_canidate_info)
                insert_nci_info(cursor, [service[0], can_base_info[0], thesaurus_canidate_info])
                insert_nci_synonyms(cursor, thesaurus_canidate_info)
            if source_type == 'nct':
                nct_to_insert = merge_brief_interv(service[0], can_base_info[0], can_base_info[3], interv_info)
                nct_to_insert.append(get_information_by_nct(service[0], can_base_info[0], nct_to_add))
                last_nct_id = get_last_id(cursor, 'art_id', 'nct_sources')
                for each_sent, each_nct_source in zip(source_data[3], source_data[4]):
                    last_nct_id = last_nct_id + 1
                    insert_nct_sources(cursor, [last_nct_id, source_data[1], source_data[2], each_sent, each_nct_source,
                                                source_data[5]])
                    nct_to_insert.append(get_information_by_nct(service[0], can_base_info[0], each_nct_source))
                insert_clinicaltrials(cursor, nct_to_insert)
            else:
                last_abst_id = get_last_id(cursor, 'art_id', 'abstract_sources')
                nct_to_insert = merge_brief_interv(service[0], can_base_info[0], can_base_info[3], interv_info)
                nct_to_insert.append(get_information_by_nct(service[0], can_base_info[0], nct_to_add))
                for each_sent, each_abst_source in zip(source_data[3], source_data[4]):
                    last_abst_id = last_abst_id + 1
                    insert_abstract_sources(cursor,
                                            [last_abst_id, source_data[1], source_data[2], each_sent, each_abst_source,
                                             source_data[5]])  # abstract insertion here
                insert_clinicaltrials(cursor, nct_to_insert)
    insert_fda_information(cursor, [fda_info, names_and_codes])
    # Закрываем подключение.
    cursor.close()
    conn.close()
    print('Closing connection')


def preprocess_blacklists(filename):
    updatedlist = []
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    cursor = conn.cursor()
    cursor.execute(f"""select cand.candidate_name
                                        from candidates cand  
                                        where cand.flag = 'Flag.black_list'""")
    existing_blacklist = [s[0] for s in cursor.fetchall()]
    with open(filename, newline="") as f:
        reader = csv.reader(f)
        titles = next(reader)
        updatedlist.append(titles)
        cand_ind = titles.index('candidate')
        for row in reader:
            if row[cand_ind] not in existing_blacklist:
                updatedlist.append(row)
    with open(filename, "w", newline="") as f:
        Writer = csv.writer(f)
        Writer.writerows(updatedlist)
        print("File has been updated")


def main(directory, mode_flag):
    print('Hello from main script')
    preprocess_blacklists(directory)
    print('Executing found_in_avicenna')
    found_therapy, found_count = execute_found_in_avicenna(directory)
    print('Executing interventions')
    found_interventions, interventions_count = execute_interventions(directory, mode_flag)
    print('Executing cancer_gov_names')
    canonical_names, brand_names, links = execute_cancer_gov_names(directory)
    print('Executing phase_taker')
    nct_phases, drug_phase_nct_brief, last_line_flag = work_on_single_file(directory, mode_flag)
    candidates = [s[0] for s in drug_phase_nct_brief]
    print('Preparing candidates for fda_labels')
    candidates_and_synonyms, names_and_codes = prepare_for_fda(candidates, directory)
    print('Executing fda_labels')
    print(len(names_and_codes))
    print(len(candidates_and_synonyms))
    for each, also in zip(names_and_codes, candidates_and_synonyms):
        print(also)
        print(each)
        print('\n')
    fda_info = work_fda_labels(candidates_and_synonyms)
    work_with_candidates_database(directory, drug_phase_nct_brief, [found_therapy, found_count], found_interventions,
                                  [canonical_names, links], fda_info, names_and_codes)


def initialize_db():
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    cursor = conn.cursor()
    main_db_actions(cursor)
    cursor.close()
    conn.close()

def process_list(list):
    for each_file in list:
        process_old_file_blacklisting('./experiments/'+each_file)

def process_old_file_blacklisting(file):
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    cursor = conn.cursor()
    print('working on ' + file)
    with open(file, 'r') as csvfile:
        #print(csvfile)
        reader = csv.reader(csvfile, dialect='excel')
        titles = next(reader)
        #print(titles)
        bl_ind = titles.index('Add_to_black_list')
        cand_ind = titles.index('candidate')
        cursor.execute("COMMIT")
        for row in reader:
            if row[bl_ind] != '':
                cursor.execute(f"""select cand.candidate_name
                                    from candidates cand 
                                    left join nct_sources ncs on cand.id = ncs.id 
                                    left join nct_batch ncb on ncs.nct_batch_id = ncb.nct_batch_id  
                                    where ncb.nct_batch_id = -1""")
                existing_blacklist = [s[0] for s in cursor.fetchall()]
                if row[cand_ind] not in existing_blacklist:
                    id_can = get_last_id(cursor, 'id', 'candidates') + 1
                    cursor.execute(f"""insert into candidates values({id_can}, '{deapostrofy(row[cand_ind])}', '{Flag.black_list}', '-', '-', '-', '-')""")
                    id_src = get_last_id(cursor, 'art_id', 'nct_sources') + 1
                    cursor.execute(
                        f"""insert into nct_sources values({id_src}, {id_can}, '{deapostrofy(row[cand_ind])}', '-', '-', -1)""")
                    cursor.execute("COMMIT")
    return

initialize_db()
a = os.listdir('./experiments')
process_list(a)
main(test_file, 1)
#process_old_file_blacklisting(test_file)
#preprocess_blacklists(test_file)
# deThesaurusify('./Thesaurus.owl', 'text_thesaurus')
# find_synonyms_for_code('C106432')
