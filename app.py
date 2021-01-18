import api

addr = "188640, ЛЕНИНГРАДСКАЯ, ВСЕВОЛОЖСКИЙ, СНТ. ТАВРЫ, Д. 422"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "РОССИЯ,197373,г. Санкт-Петербург,РАЙОН ПРИМОРСКИЙ,Город САНКТ-ПЕТЕРБУРГ,,Проспект ШУВАЛОВСКИЙ,д. 59,кор. 1,,кв. 9"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "182108, ВЕЛИКИЕ ЛУКИ, НОВЫЙ, Д.15 20"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "660042, КРАСНОЯРСКИЙ КРАЙ, Г. КРАСНОЯРСК, УЛ. СВЕРДЛОВСКАЯ, Д. 61, КВ. 25"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "157980, Костромская область, р-н. КАДЫЙСКИЙ Р-Н Поселок городского ти, , Улица БОЛЬНИЧНАЯ,  д. 18,  , кв. 1"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "630000, Новосибирская область, г. НОВОСИБИРСК, , Улица АЛЕКСАНДРА-НЕВСКОГО,  д. 6,  , кв. 10"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "190000, г. Санкт-Петербург, г. САНКТ-ПЕТЕРБУРГ, , Улица КУБИНСКАЯ, д. 28, , кв. 100"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "115114, РОССИЯ, г Москва, Павелецкий 3-й проезд, д.6, корп.А, кв.58"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "643,РОССИЯ,452920,02,БАШКОРТОСТАН РЕСП,,АГИДЕЛЬ Г,,СТУДЕНЧЕСКАЯ УЛ,14,,,20"
norm_addr = api.standardize(addr, True)
print(norm_addr)

addr = "423330, 423330,РЕСПУБЛИКА ТАТАРСТАН,Г. АЗНАКАЕВО,,УЛИЦА ШАЙХУТДИНОВА,КВ. 2, Д. 9, ,, г. АЗНАКАЕВО, , , , ,"
norm_addr = api.standardize(addr, True)
print(norm_addr)

# addr = "москва, ленина ул. , д.6"
# norm_addr = api.standardize(addr, True)
# print(norm_addr)