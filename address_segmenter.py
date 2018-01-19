#! usr/local/bin/python3
# coding: utf-8

from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np
import re
from termcolor import colored

#####
#####       Словари
#####

city_signs = {'г', 'город', 'гор'}
house_signs = {'д', 'дом', "стр", 'с', 'вл', "строение", "корпус", "корп", "к", "оф", "офис", "кв", 
  'этаж',
  'кв',
  'квартира',
  'офис',
  'оф',
  'кабинет',
  'помещение',
  'строен',
  'пом',
  'этаж', 'а', 'б', 'в', 'пом', 'комн'}

street_signs = {'пл',
  'площадь',
  'б-р',
  'бульвар',
  'линия',
  'ш',
  'шоссе',
  'ул',
  'улица',
  'пр-т',
  'пр-д',
  'проспект',
  'пр-кт','пр',
  'проезд','пер',
  'переулок',
  'наб',
  'набережная',
  'наб',
  'просп', 'аллея', 'бульвар', 'линия', 'набережная', 'переулок', 'площадь', 'проезд', 'просек', 'проспект', 'спуск', 'тупик', 'улица', 'шоссе'}

district_signs = {'район', "область", "р", "обл", "р-н", "обл-ть"}

# https://maps.vlasenko.net/russia/ru-list.csv
long_cities = pd.read_csv('ru-list.csv', sep=';', encoding='windows-1251', header=None)

long_cities.columns = ['region', 'district', 'town', 'x', 'y']
long_set_cities = set(long_cities['town'].str.lower())

country_list = {'россия', 'российская', 'федерация', 'страна'}

sep_street_signs = {
  'аллея':{'аллея', 'а'},
  'бульвар':{'б-р', 'бульвар', 'б'},
  'наб.':{'наб', 'набережная'},
  'переулок':{'пер', 'переулок'},
  'площадь':{'пл', "площадь"},
  "проспект":{"пр", "пр-кт", "просп"},
  "проезд":{"проезд", "пр-д"},
  "ул.":{"улица", "ул", "у"}
}

sep_city_signs  = {
  'city':{'г', 'гор', 'город'},
  'village':{'с', 'село', 'сел'}, #село
  'hamlet':{'д', 'дер', 'деревня', 'д-ня'},
}

sep_house_signs = {
  'home': {'вл', 'д', 'дом'},
  'bld': {'к', 'корп', 'корпус', 'с', 'стр', 'строен', 'строение'},
  'flat': {'кабинет', 'кв', 'квартира', 'комн', 'пом', 'помещение', 'комната'},
  'office': {'оф', 'офис'},
  'litera': {'а', 'б', 'в'}
}

#####
#####       Вспомогательыне методы. 
#####

def concat(items):
  '''
  Превращает вложенный список в строку
  входы:
  вложенный список
  Выход:
  строка
  '''
  if isinstance(items, list):
    result = ""
    for item in items:
      if not isinstance(item, list):
        result += str(item)
        result += ' '
      else:
        result += concat(item)
        result += ' '
    return result[:-1]
  else:
    return items

def word_ratio(string):
  """
  Возвращет отношение количества цифр в строке к общему количеству символов. 
  Вроде бы больше не используется
  """
  string = str(string)
  d=l=0
  for c in string:
    if c.isdigit():
        d=d+1
  return d/len(string)

def remove(array, element):
  '''
  Удаляет элемент из массива если он там существует. Не понятно почему родной метод работает по-другому.
  '''
  if element in array:
    array.remove(element)


####
####    DETECTORS
####

def is_city_heavy(block):
  for string in block:
    if string in long_set_cities:
      return True
  return False

def is_city(block):
  for string in block:
    if string in city_signs:
      return True
  return False

def is_street(block):
  for string in block:
    if string in street_signs:
      return True
  return False

def is_country(block):
  for string in block:
    if string in country_list:
      return True
  return False

def is_district(block):
  for string in block:
    if string in district_signs:
      return True
  return False

def is_house(block):
  for i in range(len(block)):
    string = block[i]
    if string in house_signs and i+1<len(block) and block[i+1].isdigit:
      return True
  return False

def is_index(block):
  string = concat(block)
  if len(string)!=6:
    return False
  return string.isdigit()

def predicter(block):
  '''
  Обёртка для детекторов
  '''
  if is_index(block):
    return 'index'
  elif is_country(block):
    return 'country'
  elif is_district(block):
    return 'district'
  elif is_city(block):
    return 'city'
  elif is_street(block):
    return 'street'
  elif is_house(block):
    return 'house'
  elif is_city_heavy(block):
    return 'city'
  return 'street'

def preprocess(string):
  '''
  Токенизация строки по запятым, а затем по пробелам.
  Вход:
  строка
  Выход:
  вложенный список
  '''
  for stopword in {r'\n', r'\r'}:
    string = string.replace(stopword, '')
  string = re.sub(r"[\d]+", ' \g<0> ', string)
  blocks = string.split(',')
  for i, block in enumerate(blocks):
    blocks[i] = re.findall(r'\w+(?:-\w+)+|[\w]+', str.lower(block))
  return blocks


def postprocess(address):
  '''
  Преобразует токены обратно в строку.
  '''
  new_address = {}
  for key, value in address.items():
    new_address[key] = concat(value)
  return new_address

######
######        Разделение на более мелкие составляющие
######

def cut_house(block):
  cut_ind = False
  for i in reversed(range(len(block))):
    if block[i] in house_signs or block[i].isdigit:
      if (i+1<len(block) and len(block[i+1])<5 and block[i+1].isdigit()):
        cut_ind = i
    else:
      return cut_ind
  return cut_ind

def cut_index(block):
  cut_ind = False
  for i in range(len(block)):
    if is_index(block[i]):
      cut_ind = i
      return cut_ind + 1
  return cut_ind

def cut_city(block):
  cut_ind = False
  for i in range(len(block)):
    if (block[i] in city_signs) or (block[i] in long_set_cities):
      cut_ind = i
    else:
      break
  return cut_ind + 1 if cut_ind else False

def block_split(blocks, i, j):
  '''
  Метод берёт на вход вложенный лист и разделяет на два указанный блок
  Входы:
  blocks - тот самый nested list
  i - индекс элемента(который тоже list), _который_ нужно разделить
  j - индекс элемента blocks[i], после которого нужно сделать разделение
  Пример:
  >>> block_split([[0, 1, 2], [0, 1, 2, 3]], 1, 1)
      [[0, 1, 2], [0, 1], [2, 3]]
  '''
  blocks_new = blocks[:i]
  blocks_new.append(blocks[i][:j])
  if blocks[i][j:] != []:
    blocks_new.append(blocks[i][j:])
  if blocks[i+1:] != []:
    blocks_new.extend(blocks[i+1:])
  return blocks_new

def extract_types(dic):
  dic = extract_city(dic)
  dic = extract_street(dic)
  return dic


def extract_city(dic):
  if 'city' in dic.keys():
    for token in dic['city']:
      if token in sep_city_signs['city']:
        dic['city_type'] = 'г.'
      if token in sep_city_signs['village']:
        dic['city_type'] = 'село'
      if token in sep_city_signs['hamlet']:
        dic['city_type'] = 'деревня'
    return dic
  else:
    return dic

def extract_street(dic):
  if 'street' in dic.keys():
    for token in dic['street']:
      if token in street_signs:
        for key, values in sep_street_signs.items():
          if token in values:
            dic['street_type'] = key
            remove(dic['street'], token)
            return dic
  else: return dic


def maybe_split(blocks):
  '''
  Метод находит длинные блоки (блок - то, что разделено запятой) и пытается разделить. 
  Откусывает с разных сторон и отделяет индексы и проч. Обычно отделяет всё аккурат вокруг названия улицы. Но не всегда.
  '''
  for i in range(len(blocks)):
    if len(blocks[i]) > 3:
    # легче всего отделить блоки с номером дома: они последовательны
    # начинаем с конца и отрубаем рядом стоящие номера домов и признаки номера дома
      ch = cut_house(blocks[i])
      if ch:
        blocks = block_split(blocks, i, ch)
      ci = cut_index(blocks[i])
      if ci:
        blocks = block_split(blocks, i, ci)
        i=i+1
      cc = cut_city(blocks[i])
      if cc:
        blocks = block_split(blocks, i, cc)
        i=i+1
  return blocks

def split_address(tokens):
  dic = {}
  not_used = [True for _ in range(len(tokens))] # not used yet
  if tokens[0].isdigit(): 
    dic['home'] = tokens[0]
    not_used[0] = False
  for i in range(len(tokens)):
    for key, value in sep_house_signs.items():
      if tokens[i] in sep_house_signs[key] and i+1<len(tokens) and tokens[i+1].isdigit():
        dic[key] = int(tokens[i+1])
        not_used[i] = False
        not_used[i+1] = False
    if tokens[i] in sep_house_signs['litera']:
      dic['litera'] = tokens[i]
  dic['trash'] = [tokens[i] for i in range(len(tokens)) if not_used[i]]
  if dic['trash'] == []:
    del dic['trash']
  return dic

######
######      Наведение красоты
######

def declutter(dic):
  '''
  Метод убирает слова вроде "страна", "район" из адреса. Работает как стоп-лист
  '''
  #for i in ['country', 'district', 'city', 'street', 'house']:
  if 'country' in dic and isinstance(dic['country'], list):
    remove(dic['country'], ('страна'))
  if 'district' in dic and isinstance(dic['district'], list):
    for word in district_signs:
      remove(dic['district'], word)
  if 'city' in dic and isinstance(dic['city'], list):
    for word in city_signs:
      remove(dic['city'], word)
  return dic

def Capitalize(dic):
  '''
  Делает первые буквы в именах собственных в названиях улиц заглавными
  '''
  if 'street' in dic:
    for i in range(len(dic['street'])):
      if dic['street'][i] not in street_signs:
        dic['street'][i]=dic['street'][i].capitalize()
  #easier for last ones
  for key in ['country', 'district', 'city']:
    if key in dic:
      for i in range(len(dic[key])):
        dic[key][i] = dic[key][i].capitalize()
  return dic

#####
#####       Основные методы
#####

def splitter(string):
  """
  Главный метод здесь
  На вход: строка с адресом 
  Выход: разбитый на составляющие компоненты dict с адресом.

  Примечание:
  "house" в предыдущей версии использовался как блок с цифрами дома, корпуса и т.д.
  Теперь это отдельные поля: "home", "bld" и т.д.
  """
  if len(string) < 5:
    return {}
  address = {}
  blocks = preprocess(string)
  blocks = maybe_split(blocks)
  for block in blocks:
    b_type = predicter(block)
    try:
      address[b_type].extend(block)
    except KeyError:
      address[b_type] = block
  try:
    address.update(split_address(address['house']))
  except KeyError:
    pass
  address = extract_types(address)
  address = Capitalize(declutter(address))
  return postprocess(address)
len(city_signs)

# Вывод результата: 

def colored_output(address):
  out = []
  for value, color in zip(['index', 'country', 'district', 'city_type', 'city', 'street_type', 'street', 'home',    'bld'   ], 
                          ['yellow', 'red',      'grey',      'green' ,'green',   'cyan',      'blue', 'magenta', 'magenta']):
    try:
      out.append(colored(address[value], color))
    except KeyError:
      pass
  print(', '.join(map(str,out)))


def are_same(address1, address2, threshold=80):
  """
  Метчер адресов.
  Возвращает True или False в зависимости от того одинаковые адреса или нет.
  Threshold 80 - значит одна ошибка в слове.
  """
  # Критерий одинаковости: совпали дом и улица, а остальные поля не противоречат друг другу
  if 'home' in (address1.keys() and address2.keys()):
    if fuzz.ratio(str(address1['home']), str(address2['home']))>=threshold:
      print('home: ok')
    else:
      print('home: ----')
      return False
  if 'street' in (address1.keys() and address2.keys()):
    if fuzz.ratio(address1['street'], address2['street']) >= threshold:
      print('street: ok')
      return True
    else:
      print('street: ----')
      return False
  else:
    return False
  return True








if __name__ == "__main__":
  colored_output(splitter('117485, Москва, Ул. Академика Волгина, д.4 кв 1015'))
  print(splitter('г. Москва, ул. 1-я Тверская-Ямская, д.5'))