# address-normalizer

Небольшой офлайн-парсер российских адресов для Python 3.10+. Извлекает поля,
сохраняет исходные подстроки и смещения, не требует runtime-зависимостей.

[English README](https://github.com/shigabeev/address-normalizer/blob/master/README.en.md)

## Установка

```bash
python -m pip install address-normalizer
```

## Использование

```python
from address_normalizer import parse

result = parse("г. Москва, ул. Тверская, д. 4, кв. 12")

print(result.city.value)       # Москва
print(result.street.value)     # Тверская
print(result.house_num.value)  # 4
print(result.apartment.value)  # 12
print(result.normalized)       # Москва, ул Тверская, д 4, кв 12
```

Каждый компонент содержит `value`, точную исходную подстроку `raw`, полуоткрытый
интервал `start:end`, источник решения и `confidence`. Весь результат можно
сериализовать:

```python
payload = result.as_dict()  # обычный JSON-совместимый dict
```

Неоднозначность не скрывается:

```python
result = parse("Ополченская 5-30")

print(result.normalized)     # Ополченская, д 5, кв 30
print(result.warnings)       # ("ambiguous_numeric_tail",)
print(result.alternatives)   # среди вариантов есть дом 5-30
```

`confidence` — сила решения внутри парсера, а не вероятность существования
адреса. Результаты с `warnings`, `alternatives` или `unparsed` стоит проверять.

### Поиск адреса в сообщении

```python
from address_normalizer import detect_addresses

message = "Доставить по адресу: Москва, ул. Тверская, д. 13. Позвоните."

for item in detect_addresses(message):
    print(item.text)     # Москва, ул. Тверская, д. 13
    print(item.span)     # смещение в исходном сообщении
    print(item.parsed)   # ParsedAddress
```

Детектор консервативный: лучше пропустить слабый кандидат, чем принять номер
заказа или дату за адрес.

### Несколько адресов и CLI

```python
from address_normalizer import parse_many

results = parse_many(["Тверская 1", "Невский проспект 10"])
```

```bash
address-normalizer "СПб, Невский проспект 10, корп. 2"
printf '%s\n' "Тверская 1" "Ополченская 5-30" |
  address-normalizer --jsonl
```

## Граница ответственности

Библиотека извлекает:

- индекс, регион, район, город и населённый пункт;
- улицу и тип улицы;
- дом, корпус, строение и квартиру;
- исходные смещения, неразобранный остаток, предупреждения и альтернативы.

Она не проверяет адрес по ФИАС/ГАР, не возвращает ID реестра, не исправляет
официальное написание и не геокодирует. После парсинга передайте поля в свой
актуальный resolver ФИАС/ГАР.

## Как это работает

Runtime — простой гибрид: токенизатор со смещениями, правила адресных маркеров и
чисел, компактный линейный sequence tagger для слов без маркеров, затем
детерминированная постобработка. Это не LLM и не нейросеть. Модель занимает
37 КБ; сетевых запросов и скрытых загрузок нет.

## Качество

Одна цифра «accuracy» здесь вводит в заблуждение, поэтому разные наборы
публикуются отдельно:

| Набор | Размер | Метрика | Результат |
| --- | ---: | --- | ---: |
| Историческая адресная выборка | 500 | exact component micro F1 | 95,9% |
| Шумные адресные фрагменты | 578 | span-overlap F1 | 58,7% |
| Чистые адреса по России | 100 000 | character-overlap F1 | 66,2% |
| Здания Москвы | 15 196 | exact component micro F1 | 85,4% |

Подробные определения, результаты по полям и все 500 строк с причинами ошибок:
[`benchmarks/README.md`](https://github.com/shigabeev/address-normalizer/blob/master/benchmarks/README.md).

Сильные поля — дом, корпус и строение. Слабее — административные уровни, точная
граница улицы, редкие сокращения и числовые хвосты без маркеров.

## Разработка

```bash
python -m pip install -e .
python -m pip install pytest
pytest
python tools/benchmark.py --check
```

Публичный API находится в `src/address_normalizer`, основные проверки — в
`tests`, воспроизводимый набор ошибок — в `benchmarks`.

## Лицензия

GNU GPL v3.0 only. Полный текст — в
[`LICENSE`](https://github.com/shigabeev/address-normalizer/blob/master/LICENSE).
