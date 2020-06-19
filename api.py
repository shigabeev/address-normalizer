import pandas as pd
from elasticsearch import Elasticsearch

from parsing import optimize_for_search, optimize_housenum, preprocess, extract_index, extract_house
'''
Не работает без Elastic с загруженным туда ФИАС и проиндексированным на поиск родителей каждой строки
'''
es = Elasticsearch()

def verify_address(full_address):
    '''
    Ищет адрес в ФИАС
    Вход: строка
    Выход: словарь с полным адресом по ФИАС и его составляющими
    '''
    if full_address == '':
        return []
    string = optimize_for_search(full_address)
    query = {
        'size': 1,
        "query":
            {"query_string":
                 {"fields": ["fullname"],
                  "query": string,
                  "fuzziness": "auto",
                  "use_dis_max": "true"
                  }
             }
    }
    response = es.search(index='fias_full_text', doc_type='address', body=query)

    if False:  # True чтобы добавить в ответ текст запроса
        dic = response["hits"]["hits"][0]["_source"]
        dic['query'] = string
        return dic

    try:
        return response["hits"]["hits"][0]["_source"]
    except IndexError:  # Если не найдено
        return []


def verify_home(dic, aoguid, index):
    '''
    Ищет конкретный дом на указанной улице
    '''
    query = {
        "size": 1,
        "query": {
            "bool": {
                "must": [],
                "should": [],  ## Here goes your stuff
                "must_not": []
            }
        }
    }
    cases = query['query']['bool']['should']
    must = query['query']['bool']['must']
    must_not = query['query']['bool']['must_not']
    must.append({"match": {
        "AOGUID": aoguid
    }})
    if dic.get('дом', False):
        must.append({"match": {
            "HOUSENUM": optimize_housenum(dic["дом"])
        }})
    if dic.get('корпус', False):
        cases.append({"match": {
            "BUILDNUM": optimize_housenum(dic["корпус"])
        }})
    else:
        must_not.append({"match":
                             {"BUILDNUM": "*"}
                         })
    if dic.get('строение', False):
        cases.append({"match": {
            "STRUCNUM": optimize_housenum(dic['строение'])
        }})
    else:
        must_not.append({"match":
                             {"STRUCNUM": '*'}
                         })
    if index:
        cases.append({"match": {
            "POSTALCODE": '"' + index + '"'
        }})

    must.append({"bool": {

    }})

    response = es.search(index='fias_houses', body=query)
    if len(response["hits"]["hits"]) == 0:
        dic.update({"комментарий": "дом не найден в ФИАС"})
        return dic
    else:
        response = response["hits"]["hits"][0]["_source"]

    if response["BUILDNUM"] == response["HOUSENUM"]:
        response['Корпус/строение'] = response["BUILDNUM"]
    elif len(response["BUILDNUM"]) > len(response["HOUSENUM"]):
        response['Корпус/строение'] = response["BUILDNUM"]
    elif len(response["BUILDNUM"]) > len(response["HOUSENUM"]):
        response['Корпус/строение'] = response["BUILDNUM"]
    else:
        response['Корпус/строение'] = response["BUILDNUM"]
    new_dic = {key.lower(): response[key] for key in ["HOUSENUM", "BUILDNUM", "STRUCNUM", "POSTALCODE", "HOUSEID"]}
    new_dic.update({'house query': str(query)})

    return new_dic



def standardize(string, origin=True, debug=False):
    '''
    Обёртка для всех методов выше. Разделяет адрес на его составляющие и ищет совпадение в ФИАС. В 90+% случаев находит.
    Вход: строка с адресом
    Выход: составляющие адреса
    '''
    dic = {}
    if origin:
        dic['origin'] = string

    string = preprocess(string)
    address, index = extract_index(string)
    try:
        index = index.strip()
    except AttributeError:
        pass
    address, house = extract_house(address)
    dic['index'] = index
    dic['address'] = address
    dic.update(verify_address(address))
    if dic.get('street', False):
        dic.update(verify_home(house, dic['guid'], index))
    dic.update(house)
    return dic


def get_addr(strings, progress=True):
    '''
    Обрабатывает несколько адресов подряд.
    Вход: массив строк
    Выход: pandas Dataframe
    '''
    dics = []
    n = len(strings) - 1
    for i, line in enumerate(strings):
        if progress:
            print("Working on {0} of {1}. Progress {2:03.1f}%".format(i, n, (i / n) * 100), end='\r')
        dics.append(standardize(line))
    return pd.DataFrame(dics)


if __name__ == "__main__":


    address = standardize("142703, Московская область, Ленинский район, г.Видное, ул. Школьная, д.78")
    print(address['address'] + 'д ' + address['дом'])
