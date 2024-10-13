import pandas as pd

# Задаем первую и последнюю даты в формате 'YYYY_MM_DD'
start_date = '2018-12-19'
end_date = '2019-01-20'


# Создаем список дат между первой и последней (включительно)
dates_list = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

print(dates_list)