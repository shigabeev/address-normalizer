# DISCLAIMER
# Я настоятельно рекомендую запускать этот скрипт построчно из Jupyter notebook.
# 1. Он очень долгий. Перегон dfb в csv занимает где-то 20 минут, загрузка названий улиц ещё 2, а номера домов это ещё часов на 8-10
# 2. Требует очень много памяти и можно не заметить как на машине она закончится. Таблицы с csv занимают около 50Гб, в elastic это может весить ещё около 100гб. Удаляйте dfb после того как получили csv файлы.


import os
import csv
import glob
import shutil
import argparse

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from simpledbf import Dbf5

pd.options.display.max_columns = None

es = Elasticsearch()

def load_elastic(fn, index, doc_type, encoding='cp866', es=es):
    '''
    Этот метод загружает указанный файлик в elastic.
    '''
    with open(fn, encoding=encoding) as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index=index, doc_type=doc_type, raise_on_error=False, stats_only=True)
        print('done')


# # Оптимизация под полнотекстовый поиск
def full_address(GUID):
    answer = es.search(index='fias', doc_type='address', body=
    {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {"match": {
                        "AOGUID": GUID}},
                    {"match": {
                        "ACTSTATUS": 1}}
                ]
            }
        }
    })
    entry = answer["hits"]["hits"][0]["_source"]
    string = entry['SHORTNAME'] + " " + entry['OFFNAME']
    if len(entry['PARENTGUID']) > 5:
        string = full_address(entry['PARENTGUID']) + ', ' + string
    return string


def full_address_sep(GUID, _leaf=True):
    address = {}
    address['fullname'] = ''
    answer = es.search(index='fias', doc_type='address', body=
    {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {"match": {
                        "AOGUID": GUID}},
                    {"match": {
                        "ACTSTATUS": 1}}
                ]
            }
        }
    })
    entry = answer["hits"]["hits"][0]["_source"]
    level = entry["AOLEVEL"]
    LUT = {
        '1': 'region',
        '3': 'area',
        '4': 'city',
        '5': 'district',
        '6': 'town',
        '7': 'street',
        '90': 'additional',
        '91': 'nestreet'
    }
    if _leaf:
        address['guid'] = GUID
        address['aolevel'] = entry['AOLEVEL']
    address[LUT.get(level, level)] = entry['OFFNAME']
    address[LUT.get(level, level) + "_type"] = entry['SHORTNAME']
    address['fullname'] = entry['SHORTNAME'] + " " + entry['OFFNAME']
    if len(entry['PARENTGUID']) > 5:
        nest = full_address_sep(entry['PARENTGUID'], _leaf=False)
        string = nest['fullname'] + ', ' + address['fullname']
        address.update(nest)
        address['fullname'] = string
    return address


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--fiasdir', default='fias_dbf/')
    parser.add_argument('--remove', default=False, action='store_true')
    parser.add_argument('--dont-remove', dest='remove', action='store_false')
    args = parser.parse_args()
    fias_dir = args.fiasdir

    files = glob.glob(os.path.join(fias_dir, 'ADDR*'), recursive=True)

    # # Для начала преобразуем всё в csv

    os.makedirs('fias_csv', exist_ok=True)

    files = glob.glob(os.path.join(fias_dir, 'ADDR*'), recursive=True)
    for i, f in enumerate(files):
        if f[-3:].lower() == 'dbf':
            print('processing {0} of {1}. Filename: {2}           '.format(i + 1, len(files), f), end='\r')
            dbf = Dbf5(f, codec='cp866')
            dbf.to_csv('fias_csv/ADDROBJ.csv')
            if args.remove:
                os.remove(f) # delete to save memory


#    files = ['ESTSTAT.DBF', 'FLATTYPE.DBF', 'HSTSTAT.DBF', 'INTVSTAT.DBF', 'NDOCTYPE.DBF',
#             'OPERSTAT.DBF', 'ROOMTYPE.DBF', 'SOCRBASE.DBF', 'STRSTAT.DBF']
#    for i, f in enumerate(files):
#        if f[-3:].lower() == 'dbf':
#            print('processing {0} of {1}. Filename: {2}           '.format(i + 1, len(files), f), end='\r')
#            dbf = Dbf5(os.path.join(fias_dir, f), codec='cp866')
#            dbf.to_csv('fias_csv/{0}.csv'.format(f[:-4]))

    files = glob.glob(os.path.join(fias_dir, 'HOUSE*'), recursive=True)
    for i, f in enumerate(files):
        if f[-3:].lower() == 'dbf':
            print('processing {0} of {1}. Filename: {2}           '.format(i + 1, len(files), f), end='\r')
            dbf = Dbf5(f, codec='cp866')
            dbf.to_csv('fias_csv/HOUSE.csv')
            if args.remove:
                os.remove(f) # delete to save memory

#    files = glob.glob(os.path.join(fias_dir, 'ROOM*'), recursive=True)
#    for i, f in enumerate(files):
#        if f[-3:].lower() == 'dbf':
#            print('processing {0} of {1}. Filename: {2}           '.format(i + 1, len(files), f), end='\r')
#            dbf = Dbf5(f, codec='cp866')
#            dbf.to_csv('fias_csv/ROOM.csv')

#    files = glob.glob(os.path.join(fias_dir, 'STEAD*'), recursive=True)
#    for i, f in enumerate(files):
#        if f[-3:].lower() == 'dbf':
#            print('processing {0} of {1}. Filename: {2}           '.format(i + 1, len(files), f), end='\r')
#            dbf = Dbf5(f, codec='cp866')
#            dbf.to_csv('fias_csv/STEAD.csv')

    # # Теперь надо всё это закинуть в Elastic
    # Да, это не самый оптимальный путь (можно миновать csv). Но это уже как есть


    # Загрузка самой главной таблицы
    # На первых порах её нам хватит. Остальные загружаются при надобности
    # Занимает 2 часа
    load_elastic('fias_csv/ADDROBJ.csv', 'fias', 'address')

    # Загрузка в полнотекстовый поиск, где есть и адрес и город и индекс
    df_addr = pd.read_csv('fias_csv/ADDROBJ.csv', encoding='cp866', dtype=str, error_bad_lines=False)
    # здесь могут быть ошибки парсинга на некоторых полях.
    # Их можно просто пропустить а потом попытаться исправить самостоятельно.
    start = None
    finish = None
    i = start
    for _, value in df_addr[["AOGUID"]][df_addr['ACTSTATUS'] == '1'][start:finish].iterrows():
        if i % 50 == 0:
            print(i, end="\r")
        full_addr = full_address_sep(value["AOGUID"])
        es.index(index="fias_full_text", id=value["AOGUID"], doc_type='address', body=full_addr)
        i += 1

    # На данном этапе в elastic должна быть таблица fias_full_text. Далее мы её будем максимально активно использовать

    # Можно ставить на ночь. Это очень долго: 18Гб таблица весит
    load_elastic('fias_csv/HOUSE.csv', 'fias_houses', 'home')

    # Удаляем все csv-таблицы, они теперь есть в elastic
    shutil.rmtree("fias_csv")
