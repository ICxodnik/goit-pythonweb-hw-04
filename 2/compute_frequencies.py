from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import re
import time
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
    # now we're using ThreadPoolExecutor to comply with exercise requirements
    # Probably it's not the best way to do it because of GIL and I expect that
    # there would not be any performance gain comparing to sequential execution
    # which we can check by calling this function with num_workers=1
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        max_chunk_size = len(words)//num_workers
        
        words_chunks = get_chunked_words(words, max_chunk_size)
        reduced_values = list(executor.map(map_reduce, words_chunks))
        
    # combine results from all workers
    result = {}
    for reduced_values in reduced_values:
        for key, value in reduced_values.items():
            result[key] = result.get(key, 0) + value
    return result

def map_reduce_parallel_processes(words, num_workers=4):
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        max_chunk_size = len(words)//num_workers
        
        words_chunks = get_chunked_words(words, max_chunk_size)
        reduced_values = list(executor.map(map_reduce, words_chunks))
        
    # combine results from all workers
    result = {}
    for reduced_values in reduced_values:
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
    
    
    # warm up before actual measurements
    map_reduce_parallel(words, 1)
    map_reduce_parallel(words, 4)
    map_reduce_parallel_processes(words, 1)
    
    time_start = time.time()
    result = map_reduce_parallel_processes(words, 4)
    time_end = time.time()
    print(f"Time taken 4 processes: {time_end - time_start} seconds")
    
    
    time_start = time.time()
    result = map_reduce_parallel(words, 1)
    time_end = time.time()
    print(f"Time taken 1 thread: {time_end - time_start} seconds")
    
    time_start = time.time()
    result = map_reduce_parallel(words, 4)
    time_end = time.time()
    print(f"Time taken 4 threads: {time_end - time_start} seconds")
    
    
    sorted_words = sorted(result.items(), key=lambda x: x[1], reverse=True)
    print(sorted_words[:10])
    visualize_top_words(sorted_words)
