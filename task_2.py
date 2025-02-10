import requests
import re
import matplotlib.pyplot as plt
import concurrent.futures
from collections import Counter
from multiprocessing import Pool

# Функція для отримання тексту з URL
def fetch_text(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# Функція для обробки тексту та розбиття його на слова
def tokenize(text):
    words = re.findall(r'\b\w+\b', text.lower())
    return words

# MapReduce: Функція map для підрахунку частоти слів
def map_words(chunk):
    return Counter(chunk)

# Reduce: об'єднання підрахунків
def reduce_counters(counters):
    total_counter = Counter()
    for counter in counters:
        total_counter.update(counter)
    return total_counter

# Функція для візуалізації топ N слів
def visualize_top_words(word_counts, top_n=10):
    top_words = word_counts.most_common(top_n)
    words, counts = zip(*top_words)
    
    plt.figure(figsize=(10, 5))
    plt.barh(words[::-1], counts[::-1], color='skyblue')
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title("Top 10 Most Frequent Words")
    plt.show()

def main():
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"  # Приклад: Гордiсть i упередження
    text = fetch_text(url)
    words = tokenize(text)
    
    num_chunks = 4  # Розділити текст на 4 частини для багатопроцесорної обробки
    chunk_size = len(words) // num_chunks
    chunks = [words[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]
    
    with Pool() as pool:
        mapped = pool.map(map_words, chunks)
    
    word_counts = reduce_counters(mapped)
    visualize_top_words(word_counts)

if __name__ == "__main__":
    main()
