print("Hello from phase-taking script")

import psycopg2
import sys
import os
import csv
import argparse

directory = './experiments/phase_recognition/candidates_from_2019_10_24_to_2019_10_30_checked.csv'
count_drug = 0


def dir_read(str_dir):
    file_list = []
    files = os.listdir(str_dir)
    for file in files:
        if (file[len(file) - 1] != '#') & (file[0] != '.'):
            file_list.append(str_dir + '/' + file)
    return file_list


def read_nct_from_file(filename):
    nct_list = []
    last_line_flag = False
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, dialect='excel')
        index = 0
        titles = next(spamreader)
        idx = 0
        for title in titles:
            if title in {'_ids', 'nct_ids'}:
                index = idx
            idx = idx + 1
        for row in spamreader:
            if row[index] != '':
                different_ncts = row[index].split('\'')
                nct_list.append(different_ncts[1::2])
            else:
                last_line_flag = True
    print('Last line from nct reading is empty: ', last_line_flag)
    return nct_list, last_line_flag


def db_connect():
    conn_string = "host='k8s-svc-bridge.devbg.us' port = '31575' dbname='aact' user='reader' password='aBoDOaVaTi'"
    print("Connecting to database\n->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    curs = conn.cursor()
    print("Connected!\n")
    return curs, conn


def check_phases_on_nct(nct_set, cursor):
    phases_list = []
    request_string = f"""SELECT s.nct_id, s.phase  FROM ctgov.studies s WHERE s.nct_id in {nct_set}"""
    cursor.execute(request_string)
    records = cursor.fetchall()
    for result in records:
        phases_list.append(result[1])
    return phases_list


def find_max_phase(phase_list):
    found_max = ''
    maximum = 0
    for phase in phase_list:
        if (phase != 'N/A') & (phase is not None) & (phase != 'CT_not_found'):
            if (phase[len(phase) - 1] < '6') & (phase[len(phase) - 1] >= '1'):
                if int(phase[len(phase) - 1]) > maximum:
                    found_max = phase
                    maximum = int(phase[len(phase) - 1])
        else:
            if found_max == '':
                found_max = phase
    return found_max


def find_max_drug_phase(phase_list):
    found_max = ''
    found_max_nct = ''
    maximum = 0
    for phase in phase_list:
        if (phase[1] != 'N/A') & (phase[1] is not None) & (phase[1] != 'Drug_not_found'):
            if (phase[1][len(phase[1]) - 1] < '6') & (phase[1][len(phase[1]) - 1] >= '1'):
                if int(phase[1][len(phase[1]) - 1]) > maximum:
                    found_max = phase[1]
                    found_max_nct = '=HYPERLINK("https://clinicaltrials.gov/ct2/show/' + phase[0].upper() + '"; "' + phase[0].upper() + '")'
                    maximum = int(phase[1][len(phase[1]) - 1])
        else:
            if found_max == '':
                found_max = phase[1]
                found_max_nct = phase[0]
    return found_max, found_max_nct


def insert_phase_to_csv(file_path, nct_data, drug_data, last_line_flag):
    index = 0
    pop_flag = False
    with open(file_path, newline='') as from_file:
        write_reader = csv.reader(from_file, dialect='excel')
        read_rows = list(write_reader)
        print("size of readed part = " + str(len(read_rows)))
        if read_rows[0][len(read_rows[0]) - 1] == 'max_drug_nct':
            pop_flag = True
        if pop_flag:
            read_rows[0] = read_rows[0][:-5]
        read_rows[0].append('phases_from_nct')
        read_rows[0].append('found_in_brief')
        read_rows[0].append('brief_titles(max 5)')
        read_rows[0].append('max_drug_phase')
        read_rows[0].append('max_drug_nct')
        with open(file_path, 'w') as target_file:
            writer = csv.writer(target_file)
            writer.writerow(read_rows[0])
            if last_line_flag:
                corrected_rows = read_rows[1:-1]
            else:
                corrected_rows = read_rows[1:]
            for current_row in corrected_rows:
                print("Inserting onto index = " + str(index))
                if pop_flag:
                    current_row = current_row[:-5]
                current_row.append(nct_data[index][1])
                current_row.append(drug_data[index][3][0])
                current_row.append(drug_data[index][3][1])
                current_row.append(drug_data[index][1])
                current_row.append(drug_data[index][2])
                writer.writerow(current_row)
                index = index + 1


def read_drug_from_file(filename):
    drug_list = []
    last_line_flag = False
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile, dialect='excel')
        index = 0
        titles = next(spamreader)
        idx = 0
        for title in titles:
            if title in {'candidate'}:
                index = idx
            idx = idx + 1
        for row in spamreader:
            if row[index] != '':
                #print(row[index])
                drug_list.append(row[index])
            else:
                last_line_flag = True
    print('Last line from drug reading is empty: ', last_line_flag)
    return drug_list, last_line_flag


def delete_quote(string):
    index_list = []
    idx = 0
    for char in string:
        if char == '\'':
            index_list.append(idx)
        idx = idx + 1
    for index in index_list:
        string = string[:index - 1] + string[index + 1:]
    return string


def check_phases_on_drug(drug, cursor):
    phases_list = []
    intervention_types = ('Drug', 'Biological', 'Combination Product', 'Radiation', 'Genetic')
    add_quote = '\'' + drug + '\''
    request_string = \
        f"""SELECT  s.nct_id,
                            s.phase,
                             i.name,
                             i.intervention_type,
                             ion.name,
                             s.brief_title
                        FROM ctgov.studies s
                    LEFT JOIN ctgov.brief_summaries bs ON s.nct_id = bs.nct_id
                    LEFT JOIN ctgov.design_groups dg ON s.nct_id = dg.nct_id
                    LEFT JOIN ctgov.design_group_interventions dgi ON dg.id = dgi.design_group_id
                    LEFT JOIN ctgov.interventions i ON dgi.intervention_id = i.id
                    LEFT JOIN ctgov.intervention_other_names ion ON i.id = ion.intervention_id
                    WHERE (i.intervention_type IN {intervention_types}) AND 
                            ((POSITION({add_quote.lower()} in LOWER(i.name)) > 0) OR (POSITION({add_quote.lower()} in LOWER(ion.name)) > 0))"""

    cursor.execute(request_string)
    records = cursor.fetchall()
    for result in records:
        phases_list.append([result[0], result[1]])
    return phases_list


def check_drug_in_brief(drug, cursor):
    briefs_list = []
    intervention_types = ('Drug', 'Biological', 'Combination Product', 'Radiation', 'Genetic')
    add_quote = '\'' + drug + '\''
    if len(drug) > 2:
        request_string = \
        f"""SELECT DISTINCT on (s.nct_id) s.nct_id,
                            s.phase,
                             i.name,
                             i.intervention_type,
                             ion.name,
                             s.brief_title
                        FROM ctgov.studies s
                    LEFT JOIN ctgov.brief_summaries bs ON s.nct_id = bs.nct_id
                    LEFT JOIN ctgov.design_groups dg ON s.nct_id = dg.nct_id
                    LEFT JOIN ctgov.design_group_interventions dgi ON dg.id = dgi.design_group_id
                    LEFT JOIN ctgov.interventions i ON dgi.intervention_id = i.id
                    LEFT JOIN ctgov.intervention_other_names ion ON i.id = ion.intervention_id
                    WHERE (i.intervention_type IN {intervention_types}) AND 
                            (POSITION({add_quote.lower()} in LOWER(s.brief_title)) > 0) 
                    ORDER BY  s.nct_id 
                    LIMIT 5"""
    else:
        return [[drug, 'Invalid_name', 'Invalid_name', 'Invalid_name', 'Invalid_name']]

    cursor.execute(request_string)
    records = cursor.fetchall()
    index = 1
    result_string = ''
    ncts_for_candidates = []
    for result in records:
        ncts_for_candidates.append([drug, result[0], result[1], result[5], '-'])
    if len(records) > 0:
        return ncts_for_candidates
    else:
        return [[drug, 'None', 'None', 'None', '-']]


def work_on_single_file(destination, mode_flag):
    cursor, connection = db_connect()
    unavailable_ct = []
    n = 0
    print(destination)
    file_pairs = []
    file_drug_pairs = []
    drug_ncts = []
    file_drug_list, last_line_flag = read_drug_from_file(destination)
    if mode_flag == 1:
        file_nct_list, last_line_flag = read_nct_from_file(destination)
        for nct_list in file_nct_list:
            if len(nct_list) == 1:
                nct_list.append('')
            phases = check_phases_on_nct(tuple(nct_list), cursor)
            if phases == []:
                phases = ['CT_not found']
                unavailable_ct.append([nct_list, set(phases)])
            max_phase = find_max_phase(phases)
            #print([n, nct_list, set(phases), max_phase])
            file_pairs.append([nct_list, set(phases), max_phase])
            n = n + 1

    for drug in file_drug_list:
        #print('working on ' + drug)
        drug_ncts=[]
        drug_phases = check_phases_on_drug(drug, cursor)
        drug_in_brief = check_drug_in_brief(drug, cursor)
        if mode_flag == 0:
            for phase_pair in drug_phases:
                drug_ncts.append(phase_pair[0])
            file_pairs.append([drug, drug_ncts[:min(len(drug_ncts), 4)]])
        if drug_phases == []:
            drug_phases = [['No_applicable_CT', 'Drug_not_found']]
        max_drug_phase, max_drug_nct = find_max_drug_phase(drug_phases)
        #print([n, drug, max_drug_phase, max_drug_nct], drug_in_brief[0], len(drug_in_brief[1]))
        #print(n)
        file_drug_pairs.append([drug, max_drug_phase, max_drug_nct, drug_in_brief])
        n = n + 1
    #insert_phase_to_csv(destination, file_pairs, file_drug_pairs, last_line_flag)
    if len(unavailable_ct) > 0:
        form_report([destination, unavailable_ct])
    cursor.close()
    connection.close()
    print('AACT connection closed')
    return file_pairs, file_drug_pairs, last_line_flag


def form_report(reports):
    with open('./experiments/nct_errors_report', 'w') as target_file:
        writer = csv.writer(target_file)
        for current_row in reports:
            writer.writerow(current_row)

