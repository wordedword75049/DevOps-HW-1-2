import pandas as pd
import requests
import json


URL = 'https://clinicaltrialsapi.cancer.gov/v1/interventions?name='
link_to_thesaurus = 'https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI%20Thesaurus&code='


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
            string += f'{i+1}: {space.join(lst[i]) if type(lst[i]) == list else lst[i] }'
        else:
            string += f'{i+1}: not found'
        if i != len(lst)-1:
            string += '\n'
    return string


def get_names(url: str) -> list:
    """
    This function makes a request by a given CT API url
    and gets a response in a json format.
    For an example of a response visit:
    https://clinicaltrialsapi.cancer.gov/#!/Interventions/searchInterventionsByGet
    After the response was taken, the canonical name (names), brand names (synonyms)
    and a code (codes) for the NCI thesaurus are pulled out

    Args: url - str
    ex.: https://clinicaltrialsapi.cancer.gov/v1/interventions?name=oxytocin

    Return: lists (could be a list of sublists)
    ex.: names = ["5-Aza-4'-thio-2'-deoxycytidine", 'Azacitidine', 'Azathioprine Sodium',
    'Aztreonam', 'Cedazuridine/Azacitidine Combination Agent ASTX030']
    synonyms =  [[], ['Vidaza'], ['Imuran', 'Imurel'], ['Azactam'], []]
    codes =  [['C153479'], ['C288'], ['C47961'], ['C28845'], ['C171649']]
    """
    names, synonyms, codes = [], [], []
    response = requests.get(url)
    response_string = response.content.decode('utf8')
    candidate_data = json.loads(response_string)
    candidate_data = candidate_data['terms']
    for e in candidate_data:
        names.append(e['name'])
        synonyms.append(e['synonyms'])
        codes.append(e['codes'])
    return names, synonyms, codes


def names_by_cancer_gov(df: pd.DataFrame) -> pd.DataFrame:
    """This function takes a pd.DataFrame and returns following
    lists: canonical_names, brand_names, links_to_thesaurus"""
    canonical_names, brand_names, links_to_thesaurus = [], [], []
    for i in range(len(df)):
        if pd.notna(df.candidate[i]):
            url = URL + df.candidate[i]
            names, synonyms, codes = get_names(url)
            names, synonyms = stringify(names), stringify(synonyms)
            canonical_names.append(names)
            brand_names.append(synonyms)
            if codes:
                links = [link_to_thesaurus + code[0] for code in codes]
                links = stringify(links)
            else:
                links = ''
            links_to_thesaurus.append(links)
        else:
            canonical_names.append('')
            brand_names.append('')
            links_to_thesaurus.append('')
    return canonical_names, brand_names, links_to_thesaurus


def execute_cancer_gov_names(csv_name: str) -> list:
    df = pd.read_csv(csv_name)
    return names_by_cancer_gov(df)
