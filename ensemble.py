
# coding: utf-8

# # ElasticSearch + эвристики
# Или берём лучшее от двух предыдущих версий стандартизатора адресов

import pandas as pd
import re
from elasticsearch import Elasticsearch as es
pd.options.display.max_columns = None
import spacy

'''
Не работает без Elastic с загруженным туда ФИАС и проиндексированным на поиск родителей каждой строки
'''
from elasticsearch import Elasticsearch
es = Elasticsearch()

"""
Словарь, который используется для детекции номера дома, корпуса и т.д.
"""
sep_house_signs = {
  'дом': {'вл', 'д', 'дом'},
  'корпус': {'к', 'корп', 'копр', 'кор', 'корпус', 'с', 'стр', 'строен', 'строение'},
  'квартира': {'кабинет', 'кв', 'квартира', 'ком','комн', 'пом', 'помещение', 'комната'},
  'офис': {'оф', 'офис'},
  'литера': {'а', 'б', 'в', 'е'},
  'прочее': {'литер', 'литера', 'лит'}
}

def verify_address(full_address):
  '''
  Ищет адрес в ФИАС
  Вход: строка
  Выход: словарь с полным адресом по ФИАС и его составляющими
  '''
  query = {
  "query":{
    "query_string" : {
            "fields" : ["fullname"],
            "query" : full_address,
            "use_dis_max" :  "true"
        }
  }
  }
  response = es.search(index='fias_full_text', doc_type='address', body=query)["hits"]["hits"][0]["_source"]
#   # Найти номер дома в ФИАС
#   if dic.get('Улица', False):
#     response.update(verify_home(dic, response['guid']))
#   response.update({'original':dic['original']})
  return response


def del_sp_char(string):
  '''
  Стоплист. Удаляет из строки символы переноса строки, но словарь можно дополнить при надобности
  '''
  for stopword in {r'\n', r'\r'}:
    string = string.replace(stopword, ' ')
  return re.sub(r"[\d]+", ' \g<0> ', string)

def extract_index(string, errors=False):   # 100% works !!!
  '''
  Извлекает индекс из строки
  Вход: строка
  Выход: адрес без индекса, индекс
  '''
  index = re.findall(r'[^| |,][\d]{5}[ |$|, ]', string)
  if len(index) > 1 and errors:
    print("Два индекса в строке \"%s\" ?"%string)

  if index != []:
    index = index[0]
    string = string.replace(index, '').strip()
    index = index.replace(',', '')
  else:
    index = None
  return string, index

def tokenize(string):
  '''
  Токенизатор. Раздвигает слипшиеся буквы и цифры вроде 2с3 или корп1
  Вход: строка
  Выход: массив из слов (токены)
  '''
  string = re.sub(r"[\d]+", ' \g<0> ', string)
  return re.findall(r'[\d]+|[\w]+', string)


def extract_house(string):   # trust me, it works. If doesn't — fix detector "cut_house"
  '''
  Обёртка для процедуры извлечения номера дома/корпуса от оставшейся строки
  Вход: адрес(строка)
  Выход: адрес без номеров дома/корпуса, номера дома/корпуса строкой
  '''
  tokens = tokenize(string)
  i = cut_house(tokens)
  pattern = r".?.?".join(tokens[i:])
  found = re.search(pattern, string.lower())
  if found==None:
    split = len(string)
  else:
    split = found.start()
  #split = re.search(pattern, string.lower()).start()
  address = string[:split]
  house = string[split:]
  return address, house


def search_dict(word, dic):
  '''
  Поиск по словарю. Ищет по всем вложенным рубрикам словаря и возвращает путь к найденному слову
  Вход: токен (должен быть написан маленькими буквами) и опционально словарь
  Выход: Путь к слову/несколько путей
  '''
  if isinstance(dic, dict):
    winners = []
    for key, value in dic.items():
      response = search_dict(word, value)
      if response == True:
        winners.append(key)
      elif isinstance(response, list):
        for instance in response:
          if isinstance(instance, list):
            winners.append([key, *instance])
          else:
            winners.append([key, instance])
      elif isinstance(response, str):
        winners.append([key, response])
    return winners
  elif isinstance(dic, set) or isinstance(dic, list):
    if word in dic:
      return True


def cut_house(block):
  '''
  Старый метод по обнаружению номеров дома/корпуса. Работает только с токенами
  Вход: список слов
  Выход: номер токена, с которого начинается адрес
  '''
  cut_ind = False
  for i in reversed(range(len(block))):
    block[i] = block[i].lower()
    if search_dict(block[i], sep_house_signs):
      if i+1<len(block) and (block[i+1].isdigit() or (block[i+1] in sep_house_signs['литера'])):
        cut_ind = i
    elif block[i].isdigit():
      cut_ind=i
    else: 
      return cut_ind
  return cut_ind


def standardize(string, origin = True):
  '''
  Обёртка для всех методов выше. Разделяет адрес на его составляющие и ищет совпадение в ФИАС. В 90+% случаев находит.
  Вход: строка с адресом
  Выход: составляющие адреса
  '''
  dic = {}
  address, index = extract_index(string)
  address, house = extract_house(address)
  dic['index']=index
  dic['address'] = address
  try:
    dic.update(verify_address(address))
  except:
    dic['address'] = address
  dic['house'] = house 
  if origin:
    dic['origin'] = string
  return dic


def get_addr(strings, progress=True):
  '''
  Обрабатывает несколько адресов подряд.
  Вход: массив строк
  Выход: pandas Dataframe
  '''
  dics = []
  n = len(strings) - 1
  for i,  line in enumerate(strings):
    if progress:
      print("Working on {0} of {1}. Progress {2:03.1f}%".format(i, n, (i/n)*100), end='\r')
    dics.append(standardize(line))
  return pd.DataFrame(dics)

## Методы для контроля качества


def score(df_1, ref):
  #### Не проверялось, так что может не работать
  df_1 = df_1.to_dict(orient='records')
  ref=ref.to_dict(orient='records')
  n = 0
  correct = 0
  if len(df_1) != len(ref):
    return "не совпадают размеры таблицы"
  for i, row in enumerate(ref):
    for key, value in row.items():
      if key in ["Улица"]:   #["Регион", "Индекс", "Район", "Город", "Н/п", "Улица", "Дом", "Корпус/строение"]:
        n += 1
        if value == df_1[i][key]:
          correct += 1
  return correct/n


def show_changes(df1, df2):
  '''
  Выводит список изменений между df1 и df2
  '''
  if df1.shape != df2.shape:
    print("Разные таблицы")
    return
  df1, df2 = df1.fillna(''), df2.fillna('')
  delta_cols = list(df1.columns[(df1 != df2).any(0)])
  print("Изменились столбцы " + ", ".join(delta_cols))
  n = 0
  for col in delta_cols:
    diffs = df1[col] != df2[col]
    changes = set()
    n_i = 0
    for index, changed in diffs.iteritems():
      if changed:
        out = str(df1.iloc[index][col]) + ' ==> ' + str(df2.iloc[index][col])
        n += 1
        n_i +=1
        if out not in changes:
          changes.add(out)
          print(out)
    print("{} изменений в столбце".format(n_i))
  print("Всего изменено {} значений".format(n))


if __name__ == "__main__":
  ## Проверка работоспособности

  ref = pd.read_excel('ref/sj.xlsx')
  ref2 =  pd.read_excel('ref/references.xlsx')
  
  get_addr(ref2['Исходный адрес'][:30])

