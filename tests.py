import pandas as pd
from api import get_addr

dadata_LUT = {'original': 'Исходный адрес',
              'fullname': 'Адрес',
              'index': 'Индекс',
              'postalcode': 'Индекс',
              'country': 'Страна',
              'region_type': 'Тип региона',
              'region': 'Регион',
              'area_type': 'Тип района',
              'area': 'Район',
              'city_type': 'Тип города',
              'city': 'Город',
              '65_type': 'Тип н/п',
              '65': 'Н/п',
              '???': 'Адм. округ',
              'town': 'Н/п',
              'town_type': 'Тип н/п',
              'district_type': 'Тип района',
              'district': 'Район города',
              'street_type': 'Тип улицы',
              'street': 'Улица',
              'house_type': 'Тип дома',
              #   'housenum':'Дом',
              #   'build_type':'Тип корпуса/строения',
              #   'buildnum':'Корпус/строение',
              #   'struc_type':'Тип корпуса/строения',
              #   'strucnum':'Корпус/строение',
              'flat_type': 'Тип квартиры',
              'flat_num': 'Номер Квартиры',
              'houseid': "Код ФИАС"
              }


def score(ref, orig_col="Исходный адрес", func=get_addr,
          cols_to_score=["Регион", "Город", "Н/п", "Район", "Улица", "Дом", "Корпус/строение"]):
    df_1 = func(ref[orig_col]).rename(index=str, columns=dadata_LUT)
    if 'Индекс' in cols_to_score:
        ref["Индекс"] = ref["Индекс"].fillna('9999999').astype(int).astype(str).replace('9999999', '')
    ref, df_1 = ref.fillna(''), df_1.fillna('')
    df_1 = df_1.to_dict(orient='records')
    N = ref.shape[0]
    ref = ref.to_dict(orient='records')
    n = 0
    correct = 0
    df = []
    if len(df_1) != len(ref):
        return "не совпадают размеры таблицы"
    for i, row in enumerate(ref):
        for key, value in row.items():
            if key in cols_to_score:  # ["Регион", "Индекс", "Район", "Город", "Н/п", "Улица", "Дом", "Корпус/строение"]:
                n += 1
                if str(value) == str(df_1[i][key]):
                    correct += 1
                else:
                    df.append(df_1[i])
    print("\n{0:03.1f}% correct fields".format(correct / n * 100))
    df = pd.DataFrame(df).drop_duplicates()
    print("\n{0:03.1f}% correct lines".format((N - df.shape[0]) / N * 100))
    return df


def score_by_id(ref, orig_col="Исходный адрес", func=get_addr):
    ref['Тип корпуса/строения'].astype(str)
    ref = ref[[orig_col, 'Код ФИАС']][
        (ref['Уровень по ФИАС'] == '8: дом') & (ref['Тип корпуса/строения'].astype(str) != 'nan')]
    predicted_df = func(ref[orig_col])
    true_id = ref['Код ФИАС']
    predicted_id = predicted_df['houseid']
    correct = 0
    incorrect = 0
    for true, predicted in zip(true_id, predicted_id):
        if true == predicted:
            correct += 1
        else:
            incorrect += 1
    accuracy = correct / (correct + incorrect) * 100
    print()
    print("Accuracy: {0:03.3f}%".format(accuracy))


ref = pd.read_excel('ref/references.xlsx')
