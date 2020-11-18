import requests
import json
import pandas as pd
import numpy as np
from names_by_cancer_gov import get_names


CANCERGOV_URL = 'https://clinicaltrialsapi.cancer.gov/v1/interventions?name='
DAYLIMED_URL = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?drug_name='
certificate_path = './venv/lib/python3.8/site-packages/certifi/_.nlm.nih.gov'


def stringify(lst: list) -> str:
    """
    This function takes a list as an input
    and turns it into a string
    containing sublists of elements with an ordinal number
    assigned to each sublist

    Args: lst - complex list of lists/tuples
    ex.: lst = [[e1], [e2, e3], [e4]]

    Return: a string
    ex.: '1: e1\n 2: e2, e3\n 3:e4
    """
    string, space = '', ' '
    for i in range(len(lst)):
        if any(lst[i]):
            string += f'{space.join(lst[i]) if type(lst[i]) == list else lst[i] }'
        else:
            pass
        if i != len(lst)-1:
            string += '\n'
    return string

#
# doesn't seemt to work since excel only allows one hyperlink for each cell
# def linkify(endpnt, kw, drug, download_type): 
#     return '=HYPERLINK("' + endpnt + kw + download_type + '";"' + drug + '")'
 

def get_spls(url: str) -> dict:
    """
    Returns latest spl dict from url request.
    For more information visit
    https://dailymed.nlm.nih.gov/dailymed/webservices-help/v2/spls_api.cfm
    """
    response = requests.get(url, verify=False)
    response_string = response.content.decode('utf8')
    spls_list = json.loads(response_string)['data']
    spl = spls_list[0] if spls_list else {}  # 0 is for the latest
    return spl


def get_app_num(url: str) -> str:
    """
    Returns application number from ulr request.
    For more information visit
    https://dailymed.nlm.nih.gov/dailymed/webservices-help/v2/applicationnumbers_api.cfm
    """
    response = requests.get(url, verify=False)
    response_string = response.content.decode('utf8')
    app_nums_list = json.loads(response_string)['data']
    app_num = app_nums_list[0]['application_number'] if app_nums_list else 'no num'
    return app_num


def drug_labels(drug: str):
    """
    For a given drug the application number
    link for label and label date are received
    """
    download_type = '&type=pdf'
    label_link_endpnt = 'https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?setid='
    app_num_endpnt = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/applicationnumbers.json?setid='
    daylimed_url = DAYLIMED_URL + drug
    spl = get_spls(daylimed_url)
    if spl:
        label_date = f"{drug}: {spl['published_date']}"
        label_link = f"{drug}: {label_link_endpnt}{spl['setid']}&type=pdf"
        #link = linkify(label_link_endpnt, spl['setid'], drug, download_type)
        #label_link = f"{link}"
        app_num = f"{drug}: {get_app_num(app_num_endpnt+spl['setid'])}"
    else:
        label_date = ''
        label_link = ''
        app_num = ''
    return label_date, label_link, app_num


def labels_for_candidate(candidate: str):
    """
    For each candidate in spreadsheet, there is going
    to be number of brand names and canonical names
    (see get_names fron names_by_cancer_gov).
    Hence, for a given candidate those names are found
    and then they are used a function parameters
    for drug_labels function
    """
    cand_label_dates, cand_label_links, cand_app_num = [], [], []
    cancergov_url = CANCERGOV_URL + candidate
    names, synonyms, codes = get_names(cancergov_url)
    for drug in names:
        label_date, label_link, app_num = drug_labels(drug)
        cand_label_dates.append(label_date)
        cand_label_links.append(label_link)
        cand_app_num.append(app_num)
    for syn_list in synonyms:
        for drug in syn_list:
            label_date, label_link, app_num = drug_labels(drug)
            cand_label_dates.append(label_date)
            cand_label_links.append(label_link)
            cand_app_num.append(app_num)
    return cand_label_dates, cand_label_links, cand_app_num


def labels_for_df(df: pd.DataFrame):
    """
    For each candidate in a dataframe the function
    labels_for_candidates is performed.
    Returns lists of application number, label dates and links.
    """
    df_label_dates, df_label_links, df_app_nums = [], [], []
    for i in range(len(df)):
        if pd.notna(df.candidate[i]):
            candidate = df.candidate[i]
        else:
            df_label_dates.append(np.nan)
            df_label_links.append(np.nan)
            df_app_nums.append(np.nan)
            continue
        cand_label_dates, cand_label_links, cand_app_nums = labels_for_candidate(candidate)
        cand_label_dates = stringify(cand_label_dates)
        cand_label_links = stringify(cand_label_links)
        cand_app_nums = stringify(cand_app_nums)
        df_label_dates.append(cand_label_dates)
        df_label_links.append(cand_label_links)
        df_app_nums.append(cand_app_nums)
    return df_app_nums, df_label_dates, df_label_links


def execute_daylimed_lables(csv_file: str):
    df = pd.read_csv(csv_file)
    return labels_for_df(df)


#df_app_nums, df_label_dates, df_label_links = execute_daylimed_lables('copy.csv')
