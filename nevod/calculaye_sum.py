import re


def calculate_sum_from_text(text):
    # Парсинг текста и вычисление суммы длина * число документов
    pattern = r'Длина (\d+): (\d+) документов'
    matches = re.findall(pattern, text)
    total_sum = sum(int(length) * int(count) for length, count in matches)
    return total_sum


# Пример использования функции для расчета суммы
text = """
Длина 1: 754207 документов
Длина 2: 64549 документов
Длина 3: 22315 документов
Длина 4: 10170 документов
Длина 5: 5238 документов  
Длина 6: 3045 документов  
Длина 7: 1773 документов  
Длина 8: 951 документов   
Длина 9: 370 документов 



"""
result = calculate_sum_from_text(text)
print(f"Сумма длина * число документов: {result}")
