import pandas as pd
import datetime


def read_tsv_files(fda_information_files: tuple) -> tuple:
    """
    This function reads tuple of fda related files:
    'Products.txt', 'ApplicationDocs.txt', 'MarketingStatus.txt'
    and returns of tuple of corresponding pandas dataframes
    """
    products = pd.read_csv(fda_information_files[0], sep='\t', error_bad_lines=False)
    applications = pd.read_csv(fda_information_files[1], sep='\t', error_bad_lines=False, encoding="ISO-8859-1")
    marketing_statuses = pd.read_csv(fda_information_files[2], sep='\t', error_bad_lines=False, encoding="ISO-8859-1")
    fda_dataframes = (products, applications, marketing_statuses)
    return fda_dataframes


def get_appnum_set(drug: str, products: pd.DataFrame) -> set:
    """
    This function takes a drug name and 
    returns a set of application numbers 
    corresponding to it in a products dataframe

    Example:
    
    Input:
        drug = 'oxytocin'
        products = pd.read_csv('Products.txt', sep='\t', error_bad_lines=False)
    Output:
        appnum_set = {18243, 18248, 77453, 200219, 91676}
    """
    appnum_set = set(products.query(f'"{drug.upper()}" in DrugName')['ApplNo'])
    return appnum_set


def check_marketing_status(appnum: int, marketing_statuses: pd.DataFrame):
    """
    This function takes the application number
    and checks for marketing status corresponding
    to that number in a marketing statuses dataframe
    If there is at least one status that is 1 or 2,
    the function returns True.
    Example:

    Input:
        appnum = 18243
        marketing_statuses = pd.read_csv('MarketingStatus.txt', sep='\t', error_bad_lines=False)
    Output:
        True
    """
    status_set = set(marketing_statuses.query(f"'{appnum}' in ApplNo")['MarketingStatusID'])
    acceptable_statuses = (1, 2, '1', '2')
    if status_set:
        for sts in acceptable_statuses:
            if sts in status_set:
                return True


def get_max_date_index(date_lst: list) -> int:
    """
    This function returns the index of 
    the latest date in the list of dates.
    The dates are in '%Y-%m-%d %H:%M:%S' format

    Example:
        Input: [
                '2015-04-03 00:00:00',
                '2015-06-29 00:00:00',
                '2012-07-11 00:00:00'
                ]
        Output: 1
    """
    date_lst = [datetime.datetime.strptime(f"{date}", "%Y-%m-%d %H:%M:%S") for date in date_lst]
    return date_lst.index(max(date_lst))


def get_labels_dict(appnum_set: set, applications: pd.DataFrame, marketing_statuses: pd.DataFrame) -> dict:
    """
    This function takes a set of application numbers
    and gathers information from applications and marketing statuses
    dataframes. First of all, for each number it checks whether
    marketing status is True or False. If True, it finds all the
    labels and label dates and takes the latest one.
    After creating the list consisting of latest label and its date,
    the function updates a dict like so: {appnum: labels_information}
    The data in such dictionary format is returned

    Example:
    Input:
        appnum_set = {18243, 18248, 77453, 200219, 91676}
        applications = pd.read_csv('Applications.txt', sep='\t', error_bad_lines=False)
        marketing_statuses = pd.read_csv('MarketingStatus.txt', sep='\t', error_bad_lines=False)
    Output:
        appnum_labels_dict = {18248: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2007/018248s034_ltr.pdf', '2007-07-30 00:00:00']}
    """
    appnum_labels_dict = {} 
    for num in appnum_set:
        if check_marketing_status(num, marketing_statuses):
            label_dates = list(applications.query(f'"{num}" in ApplNo')['ApplicationDocsDate'])
            labels = list(applications.query(f'"{num}" in ApplNo')['ApplicationDocsURL'])
            if label_dates:
                idx = get_max_date_index(label_dates)
                labels_information = [labels[idx], label_dates[idx]]
                appnum_labels_dict.update({num: labels_information})
    return appnum_labels_dict

def canonical_name_synonyms_fda_information(canonical_name: str, synonym_list: list, fda_dataframes: tuple) -> dict:
    """
    This function takes a canonical name and its
    list of synonyms as well as the dataframes tuple.
    Then for the name and synonyms it gathers a set of application
    numbers, for each set it gets appnum-label dictionary and finally
    it updates the dictionary corresponding to 'canonical name-synonyms'
    like so: {name: appnum-label}.
    Example:
    Input:
        canonical_name = 'Liposomal Bupivacaine'
        synonym_list = ['Bupivacaine Liposome Injectable Suspension', 'Exparel']
        fda_dataframes = read_tsv_files(fda_information_files)
    Output:
        canonical_name_synonyms_dict = 
            {
                Exparel': 
                        {
                    22496: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/022496Orig1s030ltr.pdf', '2018-11-09 00:00:00']
                        }
            }
    """
    canonical_name_synonyms_dict = {}
    canonical_appnums = get_appnum_set(canonical_name, fda_dataframes[0])
    canonical_appnum_labels_dict = get_labels_dict(canonical_appnums, fda_dataframes[1], fda_dataframes[2])
    if canonical_appnum_labels_dict:
        canonical_name_synonyms_dict.update({canonical_name: canonical_appnum_labels_dict})    
    for synonym in synonym_list:
        synonym_appnums = get_appnum_set(synonym, fda_dataframes[0])
        synonym_appnum_labels_dict = get_labels_dict(synonym_appnums, fda_dataframes[1], fda_dataframes[2])
        if synonym_appnum_labels_dict:
            canonical_name_synonyms_dict.update({synonym: synonym_appnum_labels_dict})    
    return canonical_name_synonyms_dict


def canonical_name_synonyms_dict_fda_information(canonical_synonyms_dict: dict, fda_dataframes: tuple) -> dict:
    """
    This function takes a dict consisting of 
    canonical name: synonym_list key: value pairs
    and executes canonical_name_synonyms_fda_information
    function for each key: value pair.
    Returns a dict updated in a such way that they key is
    each canonical value and the value is a result of 
    canonical_name_synonyms_fda_information function
    Example:
        Input:
        canonical_synonyms_dict =
           { 
                'Liposomal Bupivacaine': ['Bupivacaine Liposome Injectable Suspension', 'Exparel'], 
                'Bupivacaine Hydrochloride': ['Sensorcaine', 'Marcain', 'Marcaine', 'Bupivacaine'], 
            },
            fda_dataframes = read_tsv_files(fda_information_files)
        Output:
            {
                'Liposomal Bupivacaine': 
                    {
                        'Exparel':
                            {
                                22496: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/022496Orig1s030ltr.pdf', '2018-11-09 00:00:00']
                            }
                    }, 
                'Bupivacaine Hydrochloride': 
                    {
                        'Bupivacaine Hydrochloride': 
                        {
                            18053: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/018053Orig1s60Ltr.pdf', '2018-11-07 00:00:00'], 
                            22046: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/022046Orig1s009ltr.pdf', '2018-11-08 00:00:00']
                        }, '
                        Sensorcaine': 
                        {
                            18304: ['http://www.accessdata.fda.gov/drugsatfda_docs/appletter/2018/018304Orig1s049Ltr.pdf', '2018-11-07 00:00:00']
                        }, 
                        'Marcaine': {18692: ['http://www.accessdata.fda.gov/drugsatfda_docs/nda/pre96/018692Orig1s000.pdf', '2020-03-24 00:00:00']
                        }
                    }
                }
    """
    drugs_appnum_labels_dict = {}
    for canonical_name, synonym_list in canonical_synonyms_dict.items():
        if canonical_name_synonyms_fda_information(canonical_name, synonym_list, fda_dataframes):
            drugs_appnum_labels_dict.update({canonical_name: 
            canonical_name_synonyms_fda_information(canonical_name, synonym_list, fda_dataframes)})
    return drugs_appnum_labels_dict


def execute_fda_labels(canonical_synonyms_list_of_dicts: list, fda_information_files) -> list:
    """
    This function takes the list of dictionaries
    described in the canonical_name_synonyms_dict_fda_information
    documentation and returns a list of 
    canonical_name_synonyms_dict_fda_information results
    Example:
    Input:
        canonical_synonyms_list_of_dicts = list of dicts like input
        of canonical_name_synonyms_dict_fda_information function.

        fda_dataframes = read_tsv_files(fda_information_files
    Output:
        overall_drugs_appnum_labels_list = list of dicts like output
        of canonical_name_synonyms_dict_fda_information function.
    """
    overall_drugs_appnum_labels_list = []
    fda_dataframes = read_tsv_files(fda_information_files)
    for canonical_synonyms_dict in canonical_synonyms_list_of_dicts:
        drugs_appnum_labels_list = canonical_name_synonyms_dict_fda_information(canonical_synonyms_dict, fda_dataframes)
        if drugs_appnum_labels_list:
            overall_drugs_appnum_labels_list.append(drugs_appnum_labels_list)
    return overall_drugs_appnum_labels_list


product_file = 'Products.txt'
application_file = 'ApplicationDocs.txt'
marketing_status_file = 'MarketingStatus.txt'
fda_information_files = (product_file, application_file, marketing_status_file)


def work_fda_labels(dicts):
    print('fda work ready!')
    return execute_fda_labels(dicts, fda_information_files)
#canonical_names_synonyms_list_of_dicts = [  # example of entry data
#    {  # 'bupivacaine' candidate related data
#            'Liposomal Bupivacaine': ['Bupivacaine Liposome Injectable Suspension', 'Exparel'],
#            'Bupivacaine Hydrochloride': ['Sensorcaine', 'Marcain', 'Marcaine', 'Bupivacaine'],
#            'Levobupivacaine Hydrochloride': ['Chirocaine'],
#            'Extended Release Bupivacaine Hydrochloride Resorbable Matrix Formulation': ['Sustained Release Bupivacaine Hydrochloride', 'ER Bupivacaine HCl', 'SABER-Bupivacaine Hydrochloride'],
#            'Bupivacaine': ["1-Butyl-2',6'-pipecoloxylidide", "AH 250"],
#            'Levobupivacaine': ["(S)-1-Butyl-N-(2,6-Dimethylphenyl)-2-Piperidinecarboxamide", "(S)-1-Butyl-2',6'-Pipecoloxylidide"]
#    },
#    {
#        'proxalutamide': ['proxalutamide']
#    }
#]
#overall_drugs_appnum_labels_list = execute_fda_labels(canonical_names_synonyms_list_of_dicts, fda_information_files)
#print(overall_drugs_appnum_labels_list)  # intended list, should be of right format
