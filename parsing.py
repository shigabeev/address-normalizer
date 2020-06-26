import re
import itertools
import operator

import pandas as pd
from elasticsearch import Elasticsearch

def inverdic(dic):
    resdic = {}
    for key, value in dic.items():
        for index in value:
            if type(value) == set or type(value) == list:
                if index in resdic.keys():
                    if isinstance(resdic[index], set):
                        resdic[index].add(key)
                    elif isinstance(resdic[index], list):
                        resdic[index].append(key)
                    else:
                        resdic[index] = [resdic[index]]
                        resdic[index].append(key)
                else:
                    resdic[index] = key
            elif type(value) == dict:
                resdic.update(inverdic(value))
    return resdic


"""
Словарь, который используется для детекции номера дома, корпуса и т.д.
"""
sep_house_signs = {
    'дом': {'д', 'дом'},
    'владение': {'владение', 'вл'},
    'корпус': {'к', 'корп', 'копр', 'кор', 'корпус'},
    'строение': {'с', 'стр', 'строен', 'строение'},
    'квартира': {'кв', 'квартира'},
    'помещение': {'пом', 'помещение'},
    'комната': {"ком", 'комн', 'комната'},
    'кабинет': {"кабинет", "каб", "к-т", "каб-т"},
    'офис': {'оф', 'офис'},
    'литера': set("абвежз"),
    'прочее': {'литер', 'литера', 'лит'},
    'дробь': {'/', '-'}
}

house_signs_inv = inverdic(sep_house_signs)


def boost_keyword(dic):
    replaces_lowered = {}
    for key, value in replaces.items():
        if isinstance(key, tuple):
            new_key = "(" + ' OR '.join(key) + ')^' + str(1 / len(key))
            replaces_lowered[new_key] = value
        else:
            replaces_lowered[key] = value
    return replaces_lowered


'''
Этот словарь приводит все типы адресных объектов к стандартному виду (к тому что в ФИАС)
'''
replaces = {
    'обл': {"область", "обл", "обл-ть"},
    'респ': {"республика", 'респ'},
    'край': {'край'},
    'г': {'г', 'гор', 'город'},
    ('ао', 'а.окр'): {'автономный округ', "автономный", 'аокр', 'а.окр'},
    ('а.обл', 'аобл'): {'автономная область', 'авт.обл', 'аобл', 'а обл', 'аобл'},

    ('аллея', 'ал'): {'аллея', 'а', 'ал'},
    'б-р': {'б-р', 'бульвар'},
    'наб': {'наб', 'набережная'},
    'пер': {'пер', 'переулок'},
    ('площадь', 'пл'): {'пл', "площадь"},
    ('проспект', 'пр-кт'): {"проспект", "пр", "пр-кт", "просп", 'пр-т'},
    "пр-д": {"проезд", "пр-д", "прд"},
    "ул": {"улица", "ул", "у", 'ул-ца'},

    'р-н': {'район', "р", "р-н"},
    'п': {'поселок', 'посёлок', "пос"},
    'пгт': {'поселок городского типа', 'посёлок городского типа', 'пос. гор. типа', 'пос.гор.типа', 'пос гор типа'},

    'г': {'г', 'гор', 'город'},
    'с': {'с', 'село', 'сел'},
    'д': {'д', 'дер', 'деревня', 'д-ня'},
    'с/п': {"сельский поселок", "сельский посёлок", "сп", 'сельское поселение', 'сельпо', 'сп', 'сел.п.', }
}
replaces_inv = inverdic(boost_keyword(replaces))


def del_sp_char(string):
    '''
    Стоплист. Удаляет из строки символы переноса строки, но словарь можно дополнить при надобности
    '''
    for stopword in {r'\n', r'\r', '\\', '(', ')', ':'}:
        string = string.replace(stopword, ' ')
    return re.sub(r"[\d]+", ' \g<0> ', string)


def preprocess(string):
    '''
    Отделяет всё что можно друг от друга чтобы облегчить токенизацию
    '''
    string = del_sp_char(string)
    # Превращает "2c3" в "2 c 3"  и  "2-3" в "2 - 3"
    string = re.sub(r"[\d]+|[\W]+", ' \g<0> ', string)

    string = string.replace(',', ', ')  # то же самое, только с запятыми
    string = re.sub(r'\,|\.|\-|\'|\"|\(|\)', '', string)
    string = re.sub(r' +', ' ', string)
    return string


def multiple_replace(dict, text, compiled=False):
    '''
    Преобразует словарь замен (dict) в паттерн замен для регулярок и тут же применяет его
    '''
    if not compiled:
        regex = re.compile(r"\b(%s)\b" % "|".join(map(re.escape, dict.keys())))
    else:
        regex = compiled

    # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)


def tokenize(string, comma=False):
    '''
    Токенизатор. Раздвигает слипшиеся буквы и цифры вроде 2с3 или корп1
    Вход: строка
    Выход: массив из слов (токены)
    '''
    # string = re.sub(r"[\d]+", ' \g<0> ', string).lower() — это уже сделано при препроцессинге
    string = string.lower()
    if not comma:
        return re.findall(r'[\d]+|[\w]+', string)
    if comma:
        return re.findall(r'[\d]+|[\w]+|\,', string)


def tokens_to_string(tokens, string):
    '''
    Ищет токены в строке и возвращает их позицию начала
    '''
    pattern = r".?.?".join(tokens)
    found = re.search(pattern, string.lower())
    if found == None:
        split = len(string)
    else:
        split = found.start()
    return split


def extract_index(string, errors=False):  # 100% works !!!
    '''
    Извлекает индекс из строки
    Вход: строка
    Выход: адрес без индекса, индекс
    '''
    index = re.findall(r'[^| |,][\d]{5}[ |$|, ]', string)
    if len(index) > 1 and errors:
        print("Два индекса в строке \"%s\" ?" % string)

    if index != []:
        index = index[0]
        string = string.replace(index, '').strip()
        index = index.replace(',', '')
    else:
        index = None
    return string, index


def clarify_address(tokens, types):
    '''
    Разбивает строку с номером дома на ещё более точные части.
    Возвращает номер дома, корпуса и строения
    '''
    # add missing "number" type
    for i, token in enumerate(tokens):
        if token.isdigit():
            types[i] = 'число'
            if i == 0:
                types[i] = "дом"
            elif types[i - 1] in sep_house_signs and types[i - 1] != 'литера' and types[i - 1] != 'дробь':
                types[i] = types[i - 1]
            elif i >= 2 and types[i - 1] == 'дробь':
                types[i] = types[i - 2]
                types[i - 1] = types[i - 2]
        elif types[i] == 'литера' and i != 0:
            tokens[i - 1] += tokens[i]

    # write this info somewhere
    dic = {}
    for token, typ in zip(tokens, types):
        if token not in house_signs_inv and typ != 'препинания':
            if typ not in dic:
                dic[typ] = token

    # rename
    dic['Дом'] = dic.get('дом', '')
    if len(dic.get('корпус', '')) > len(dic.get('строение', '')):
        dic['Корпус/строение'] = dic['корпус']
    elif dic.get('строение', False):
        dic['Корпус/строение'] = dic['строение']
    return dic


def extract_house_tokens(tokens):
    '''
    находит последовательность номеров дома/корпуса/строения среди токенов и возвращает их
    '''
    a = lambda x: "число" if x.isdigit() and len(x) < 6 else "препинания" if x == ',' else "не распознано"
    types = [house_signs_inv.get(x, a(x)) for x in tokens]
    types_bin = [0 if x == 'не распознано' else 1 for x in types]
    array = list((list(y) for (x, y) in itertools.groupby((enumerate(types_bin)), operator.itemgetter(1)) if x == 1))
    if len(array) == 0:
        return [], []
    longest_seq = max(reversed(array), key=len)
    return [tokens[i] for (i, _) in longest_seq], [types[i] for (i, _) in longest_seq]


def extract_house(string):  # from 2.0
    '''
    Обёртка для процедуры извлечения номера дома/корпуса от оставшейся строки
    Вход: адрес(строка)
    Выход: адрес без номеров дома/корпуса, номера дома/корпуса строкой
    '''
    tokens = tokenize(string, comma=True)
    house_tokens, house_types = extract_house_tokens(tokens)

    split = tokens_to_string(house_tokens, string)

    house = clarify_address(house_tokens, house_types)
    address = string[:split]
    # house = string[split:]
    return address, house


stopwords = {
    'российская': '',
    'федерация': '',
    'орел': 'орёл',
    'мо': 'московская обл',
    'большой': "(б OR большой)",
    'большая': "(б OR большая)",
    'малый': "(м OR малый)",
    'малая': "(м OR малая)",
    'средний': '(ср OR с OR средний)',
    'средняя': '(ср OR с OR средняя)',
    'нижний': '(н OR нижний)',
    'б': "(б OR большая OR большой)",
    'с': '(ср OR с OR средняя OR средний)',
    'ср': '(ср OR с OR средняя OR средний)',
    'м': "(м OR малый OR малая)",
    'н': '(н OR нижний)',
    '/': ''
}


def optimize_for_search(string):
    '''
    вводит небольшие изменения в строку поиска для более точного поиска
    '''
    string = string.replace('ё', 'е')
    string = multiple_replace(stopwords, string.lower())
    string = multiple_replace(replaces_inv, string)
    # string = re.sub(r"[а-яА-Я]{4,}", '\g<0>~^2', string)
    return string


housenum_replaces = {
    '/': '\/'
}


def optimize_housenum(string):
    '''
    Пока что делает escape для "/"
    '''
    # creates escape characters for elasticsearch
    string = multiple_replace(housenum_replaces, string)
    return '"' + string + '"'
