from collections import defaultdict
import re
import requests

def map_function(text: str):
    words = re.findall(r'[a-z]+', text.lower(), re.IGNORECASE)
    return [(word, 1) for word in words]

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(shuffled_values):
    reduced = {}
    for key, values in shuffled_values:
        reduced[key] = sum(values)
    return reduced

# Виконання MapReduce
def map_reduce(text):
    # Крок 1: Мапінг
    mapped_values = map_function(text)

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Крок 3: Редукція
    reduced_values = reduce_function(shuffled_values)

    return reduced_values

if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    
    response = requests.get(url)
    text = response.text
    
    result = map_reduce(text)

    print("Результат підрахунку слів:", result)
