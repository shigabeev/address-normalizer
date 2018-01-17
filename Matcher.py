#! /usr/bin/python3
# coding: utf-8

# Результаты:
# 1. Сплиттер очень эффективно (более 20к строк в секунду) разделяет строки адресов на составляющие
# 2. Для хорошо размеченных данных (как SJ) точность близка к 100%, на госзаказах в районе 90%
# 3. Думаю, даже этого хватит чтобы связать бо́льшую часть данных.
# Планы:
# 1. Увеличить точность с помощью разбиения уже получившихся блоков (если длинна блока > 20 символов, можно делить)
# 2. рафинировать результат под нужды Matchera и запустить его

import re


def concat(items):
    result = ""
    for item in items:
        if not isinstance(item, list):
            result += str(item)
            result += ' '
        else:
            result += concat(item)
            result += ' '
    return result[:-1]


def purify(field):
    if isinstance(field, str):
        return re.sub(r'[\[\],\'\"]', '', field)
    elif isinstance(field, list):
        return concat(field)
    else:
        return field

city_signs = ['г', 'город', 'гор', 'c']

street_signs = ['пл',
                'площадь',
                'б-р',
                'бульвар',
                'линия',
                'ш',
                'шоссе',
                'ул',
                'улица',
                'пр-т',
                'проспект',
                'пр-кт', 'пр',
                'проезд', 'пер',
                'переулок',
                'наб',
                'набережная',
                'наб',
                'просп']

house_signs = ['д', 'дом', "стр", "строение", "корпус", "корп", "к", "оф", "офис", "кв",
               'этаж',
               'кв',
               'квартира',
               'офис',
               'оф',
               'кабинет',
               'помещение',
               'строен',
               'пом',
               'наб',
               'этаж']


def word_ratio(string):
    string = str(string)
    d = l = 0
    for c in string:
        if c.isdigit():
            d = d + 1
    return d / len(string)


def is_city(block):  # нужен лонг-лист по городам, обозначений явно не хватает
    for string in block:
        for sign in city_signs:
            if string == sign:
                return True
    return False


def is_street(block):  # на маленьком наборе 100% точность
    for string in block:
        for sign in street_signs:
            if string == sign:
                return True
    return False


def is_house(block):  # тоже 100%
    for string in block:
        for sign in house_signs:
            if string == sign:
                return True
    return False


def is_index(block):
    string = concat(block)
    if len(string) < 5:
        return False
    if word_ratio(string) == 1:
        return True
    return False


def predicter(block):
    if is_index(block):
        return 'index'
    elif is_city(block):
        return 'city'
    elif is_street(block):
        return 'street'
    elif is_house(block):
        return 'house'
    return 'trash'


def preprocess(string):
    for stopword in [r'\n', r'\r']:
        string = string.replace(stopword, '')
    blocks = string.split(',')
    for i, block in enumerate(blocks):
        blocks[i] = re.findall(r"[\w']+", str.lower(block))
    return blocks


def postprocess(address):
    new_address = {}
    for key, value in address.items():
        new_address[key] = concat(value)
    return new_address


def splitter(string):
    # IN
    # String that contains address
    # OUT
    # dictionary with address splitted into fields

    if len(string) < 5:
        return {}
    address = {}
    for block in preprocess(string):
        #maybe split
        b_type = predicter(block)
        try:
            address[b_type].append(block)
        except KeyError:
            address[b_type] = block
    return postprocess(address)


if __name__ == '__main__':
    address = '143406, Московская область, г. Красногорск, ул. 50 лет Октября, д.12'
    print(splitter(address))
