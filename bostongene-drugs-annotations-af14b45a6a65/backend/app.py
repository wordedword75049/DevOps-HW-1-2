from enum import Enum
from flask import Flask, request
import psycopg2
import json
from service_functions import dictify_candidate, dictify_candidate_info, myconverter, get_last_id, get_cand_by_nct_batch, fill_dict, get_cand_no_batch

app = Flask(__name__)
conn_string = "host='localhost' dbname='postgres' user='postgres' password='12345'"


class Flag(Enum):
    No_flag = -1
    TP = 1
    FP = 2
    black_list = 3

@app.route('/')
def hello():
    return "Hello World!"


@app.route('/update', methods = ['PUT', 'GET'])
def update_flag():
    id_c= request.args.get('id', None)
    new_flag = request.args.get('nflag', None)
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()
    if new_flag == 'tp':
        curs.execute(f"""update candidates set flag = '{Flag.TP}' where id = {id_c}""")
    elif new_flag == 'bl':
        curs.execute(f"""update candidates set flag = '{Flag.black_list}' where id = {id_c}""")
    elif new_flag == 'fp':
        curs.execute(f"""update candidates set flag = '{Flag.FP}' where id = {id_c}""")
    elif new_flag == 'none':
        curs.execute(f"""update candidates set flag = '{Flag.No_flag}' where id = {id_c}""")
    curs.execute("COMMIT")
    return "Updated " + str(id_c) + "`s flag to " + new_flag


@app.route('/sources')
def sources():
    res_list = []
    source_keys = ('id', 'label', 'link_self')
    nct_data = ('nct', 'NCT', '/sources/nct')
    res_list.append(fill_dict(source_keys, nct_data))
    conf_data = ('conference', 'Conference', '/sources/conference')
    res_list.append(fill_dict(source_keys, conf_data))
    return json.dumps({'sources': res_list})

@app.route('/sources/<id>')
def exact_source(id):
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()
    if id == 'nct':
        source_keys = ('id', 'label', 'useBatchMode', 'link_self', 'link_batches', 'link_latest_batch', 'link_latest_candidates')
        last_id = get_last_id(curs, 'nct_batch_id', 'nct_batch')
        id_data = (id, 'NCT', True, '/sources/nct', '/batches?source=nct', f'/batches/{last_id}', f'/candidates?source=nct&batchId={last_id}')
    else:
        source_keys = ('id', 'label', 'useBatchMode', 'link_self', 'other_info')
        id_data = ('conference', 'Conference', False, '/sources/conference', 'TBA')

    result = fill_dict(source_keys, id_data)
    return json.dumps(result)


@app.route('/batches', methods = ['GET'])
def batch_list():
    source = request.args.get('source', None)
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()
    result = []
    if source == 'nct':
        curs.execute('select nct_batch_id, label, batch_creation_date from nct_batch  where nct_batch_id != -1 order by batch_period_end')
        records = curs.fetchall()
        source_keys = ('id', 'label', 'creation_date', 'link_self', 'link_candidates')
        for each in records:
            each = each + (f'/batches/{each[0]}',)
            each = each + (f'/candidates?source=nct&batchId={each[0]}',)
            result.append(fill_dict(source_keys, each))

    else:
        result.append('Abstracts not ready')
    return json.dumps({'batches': result})


@app.route('/batches/<id>')
def batch(id):
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()
    curs.execute(
        f"""SELECT distinct(nct_batch_id),
            label,
            batch_creation_date,
            batch_period_start,
            batch_period_end
            from nct_batch
            where nct_batch_id = {id}
            order by (nct_batch_id)""")
    batch = curs.fetchone()
    source_keys = ('id', 'label', 'creation_date', 'period_start', 'period_end', 'link_self', 'link_candidates')
    if batch is not None:
        batch = batch + (f'/batches/{batch[0]}',)
        batch = batch + (f'/candidates?source=nct&batchId={batch[0]}',)
        result = fill_dict(source_keys, batch)
    else:
        result = 'No such batch'
    return json.dumps(result)


@app.route('/candidates', methods = ['GET'])
def drugs():
    source = request.args.get('source', None)
    batch = request.args.get('batchId', None)
    print(source, batch)

    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()
    if (source is not None) & (batch is not None):
        return get_cand_by_nct_batch(curs, conn, batch)
    elif source is not None:
        return get_cand_no_batch(curs, conn)
    else:
        return "Waiting for source choice"

@app.route('/candidates/<id>')
def drug_by_id(id):
    print('Connecting to candidates database')
    conn = psycopg2.connect(conn_string)
    print('Connected!')
    curs = conn.cursor()

    curs.execute(f"""SELECT n.nct_id, ct.brief_title, n.sentences from candidates cand LEFT JOIN nct_sources n on n.id = cand.id LEFT JOIN clinicaltrials_information ct on ct.nct_id = n.nct_id where cand.id = {id}""")
    nct_sources = curs.fetchall()

    curs.execute(
        f"""SELECT ct.nct_id, ct.brief_title, ct.nct_phase from candidates cand LEFT JOIN clinicaltrials_information ct on ct.id = cand.id where cand.id = {id} and ct.nct_id = cand.max_phase_nct""")
    max_drug_phase_info = curs.fetchone()
    if max_drug_phase_info is None:
        max_drug_phase_info = ['-', '-', '-']

    curs.execute(
        f"""SELECT cand.flag, cand.candidate_name from candidates cand where cand.id = {id}""")
    flag_info = curs.fetchone()

    curs.execute(
        f"""SELECT ct.nct_id, ct.brief_title, ct.nct_phase, ct.interventions_w_candidate from candidates cand LEFT JOIN clinicaltrials_information ct on ct.id = cand.id where cand.id = {id}""")
    clinicaltrial_info = curs.fetchall()

    curs.execute(
         f"""select
            ncodes.canonical_name,
            ncodes.nci_thesaurus_code,
            nsyns.term,
            nsyns.source,
            nsyns.type,
            fdainf.drug_name,
            fdainf.drug_application,
            fdainf.fda_label_date,
            fdainf.fda_label_link
            from candidates cand
            left join nci_information ninf on cand.id = ninf.id
            left join nci_codes ncodes on ninf.nci_thesaurus_code = ncodes.nci_thesaurus_code
            left join nci_synonyms nsyns on nsyns.nci_thesaurus_code = ncodes.nci_thesaurus_code
            left join fda_information fdainf on fdainf.nci_thesaurus_code = ncodes.nci_thesaurus_code
            where cand.id = {id}""")
    nci_info = curs.fetchall()

    curs.execute(
        f"""SELECT cand.count_in_avicenna, cand.found_in_avicenna from candidates cand where cand.id = {id}""")
    avicenna_info = curs.fetchone()


    curs.close()
    conn.close()
    # counts = []
    # for each, count_can in zip(records, counts):
    #     main_list.append(dictify_candidate_info(each, count_can))
    dictified_records = dictify_candidate_info(id, 1, flag_info[1], flag_info[0], nct_sources, max_drug_phase_info, clinicaltrial_info, nci_info, avicenna_info)
    a = dictified_records
    #for each in a.items():
    #    print('______________________________________________________________________________________')
    #    if hasattr(each[1], '__iter__') & (type(each[1]) is not dict) & (type(each[1]) is not str):
    #        print(each[0])
    #        for each_each in each[1]:
    #            print(each_each)
    #    else:
    #        print(each)
    #    print('\n')
    #a = list(a.items())[4][1]
    #for each in a:
    #    print('______________________________________________________________________________________')
    #    for eeach in each.items():
    #        print(eeach)
    #    print('\n')
    return json.dumps(dictified_records, default = myconverter)


if __name__ == '__main__':
    app.run()
