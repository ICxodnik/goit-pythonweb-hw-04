from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import re
import requests
import matplotlib.pyplot as plt

def map_function(words):
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

def map_reduce(words):
    mapped_values = map_function(words)

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Крок 3: Редукція
    reduced_values = reduce_function(shuffled_values)

    return reduced_values

def get_chunked_words(words, max_chunk_size):
    words_chunks = [words[i:i+max_chunk_size] for i in range(0, len(words), max_chunk_size)]
    return words_chunks

def map_reduce_parallel(words, num_workers=4):
    # compute complexity is not enough to justify parallelization comparing to the cost 
    # of copying data between processes but it's a good exercise to understand how to use ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        max_chunk_size = len(words)//num_workers
        
        words_chunks = get_chunked_words(words, max_chunk_size)
        reduced_values = list(executor.map(map_reduce, words_chunks))
        
    # combine results from all workers
    result = {}
    for reduced_values in reduced_values:
        print("len(reduced_values)", len(reduced_values))
        for key, value in reduced_values.items():
            result[key] = result.get(key, 0) + value
    return result

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
    
    words = re.findall(r'[a-z]+', text.lower(), re.IGNORECASE)
    
    result = map_reduce_parallel(words)
    
    sorted_words = sorted(result.items(), key=lambda x: x[1], reverse=True)
    print(sorted_words[:10])
    visualize_top_words(sorted_words)
