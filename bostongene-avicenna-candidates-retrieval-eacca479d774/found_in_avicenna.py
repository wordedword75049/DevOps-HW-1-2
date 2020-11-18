import csv

import pandas as pd
import numpy as np
import subprocess

from bgnlp.core import Standoff
from bgnlp.components import DrugRecognition, TextProcessing


def stringify(lst: list) -> str:
    """
    This function takes a list as an input
    and turns it into a string
    containing sublists of elements with an ordinal number
    assigned to each sublist

    Args: lst - complex list of lists
    ex.: lst = [[e1], [e2, e3], [e4]]

    Return: a string
    ex.: '1: e1\n 2: e2, e3\n 3:e4
    """
    string, space = '', ' '
    for i in range(len(lst)):
        if any(lst[i]):
            string += f'{space.join(lst[i]) if type(lst[i]) == list else lst[i] }'
        else:
            string += f'not found'
        if i != len(lst)-1:
            string += '\n'
    return string


class DrugsPipeline:
    def __init__(self, path_to_dict):
        self.text_processing = TextProcessing()
        self.drug_recognizer = DrugRecognition(data_path=path_to_dict)

    def __call__(self, text):
        self.standoff = Standoff(text=text)
        self.text_processing.patch_standoff(self.standoff, self.text_processing(self.standoff))
        return self.drug_recognizer(self.standoff)["drug_recognition"]["drug_canonical_names"]
        pass


def get_dictionary():
    """
    Gets up-to-date dictionary from the following repository.
    Bitbucket username and passwords are required.
    """
    bash_command = "dvc get https://bitbucket.org/bostongene/nlppipeline_dvc_repo.git drug_dicts.csv"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output, error)


def found_in_avicenna(df: pd.DataFrame) -> pd.DataFrame:
    """
    Every candidate in a dataframe is checked
    if it is in the dictionary or not.
    Returns arrays of matches and their count. 
    """
    found_in_avicenna_lst, found_in_avicenna_count = [], []
    for i in range(len(df)):
        candidate = df.candidate[i]
        try:
            result = drugs_pipeline(candidate)
        except TypeError:
            result = []
        if result:
            found_in_avicenna_lst.append(stringify(result))
            found_in_avicenna_count.append(len(result))
        else:
            found_in_avicenna_lst.append('not found')
            found_in_avicenna_count.append(0)
    return found_in_avicenna_lst, found_in_avicenna_count


get_dictionary()
drugs_pipeline = DrugsPipeline('drug_dicts.csv')


def execute_found_in_avicenna(csv_name: str) -> list:
    df = pd.read_csv(csv_name)
    return found_in_avicenna(df)


#found_in_avicenna_lst, found_in_avicenna_count = execute_found_in_avicenna(csv)
