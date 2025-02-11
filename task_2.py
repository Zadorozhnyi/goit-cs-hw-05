import string
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import matplotlib.pyplot as plt

def get_text(url):
    try:
        response = requests.get(url)
        # Перевірка на помилки HTTP
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        return None

# Функція для видалення знаків пунктуації
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# Виконання MapReduce
def map_reduce(text):
    # Видалення знаків пунктуації
    text = remove_punctuation(text)
    words = text.split()

    # Крок 1: Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Крок 3: Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

# Функція для візуалізації топ 10 слів
def visualize_top_words(word_counts):
    top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    words, counts = zip(*top_words)
    
    plt.figure(figsize=(10, 5))
    plt.barh(words[::-1], counts[::-1], color='skyblue')
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title("Top 10 Most Frequent Words")
    plt.show()
    
def process_text(url):
    # Вхідний текст для обробки
    text = get_text(url)
    if text:
        # Виконання MapReduce на вхідному тексті
        result = map_reduce(text)
        print("Результат підрахунку слів:", result)
        
        # Візуалізація
        visualize_top_words(result)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")


if __name__ == '__main__':
    process_text("https://gutenberg.net.au/ebooks01/0100021.txt")
    process_text("https://www.gutenberg.org/files/1342/1342-0.txt")
