import re


def calculate_sum_from_text(text):
    # Парсинг текста и вычисление суммы длина * число документов
    pattern = r'Длина (\d+): (\d+) документов'
    matches = re.findall(pattern, text)
    total_sum = sum(int(length) * int(count) for length, count in matches)
    return total_sum


# Пример использования функции для расчета суммы
text = """
Длина 1: 760838 документов
Длина 2: 66212 документов
Длина 3: 23006 документов
Длина 4: 10267 документов
Длина 5: 5218 документов
Длина 6: 3102 документов
Длина 7: 1878 документов
Длина 8: 1045 документов
Длина 9: 396 документов

 




"""
result = calculate_sum_from_text(text)
print(f"Сумма длина * число документов: {result}")
