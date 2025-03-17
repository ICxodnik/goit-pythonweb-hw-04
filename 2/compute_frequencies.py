from collections import defaultdict
import re
import requests
import matplotlib.pyplot as plt

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

def visualize_top_words(result, num_words=10):
    plt.bar([x[0] for x in result[:num_words]], [x[1] for x in result[:num_words]])
    plt.xlabel("Words")
    plt.ylabel("Frequencies")
    plt.title(f"Top {num_words} words")
    plt.show()

if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    
    response = requests.get(url)
    text = response.text
    
    result = map_reduce(text)
    
    sorted_words = sorted(result.items(), key=lambda x: x[1], reverse=True)
    print(sorted_words[:10])
    visualize_top_words(sorted_words)
