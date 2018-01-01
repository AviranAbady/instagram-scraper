import sys
import ast
import json
import shutil
import heapq
import datetime
import mmap
import time
# from tqdm import tqdm
from collections import defaultdict
from io import StringIO


def write_file(data, suffix):
    file_name = 'output/out_' + str(suffix) + ".bulk"
    print("writing " + file_name)
    with open(file_name, 'w') as fd:
        data.seek(0)
        shutil.copyfileobj(data, fd)
        fd.close()


create_edge_counter = 0


def create_edge(node_a, node_b, graph):
    global create_edge_counter
    create_edge_counter += 1
    if node_a not in graph:
        graph[node_a] = {node_b: 1}
    elif node_b in graph[node_a]:
        graph[node_a][node_b] += 1
    else:
        graph[node_a][node_b] = 1


def add_to_graph(data, graph):
    ignore_words = ['follow', 'like', 'tag', 'day', 'insta']
    length = len(data)
    for i in range(0, length):
        for j in range(i + 1, length):
            node_a = data[i]
            node_b = data[j]
            skip = False
            for word in ignore_words:
                if word in node_b:
                    skip = True
                    break

            if not skip:
                create_edge(node_a, node_b, graph)

            create_edge(node_b, node_a, graph)


def inline_add_to_graph(data, graph):
    length = len(data)
    for i in range(0, length):
        for j in range(i + 1, length):
            node_a = data[i]
            node_b = data[j]
            if node_a not in graph:
                graph[node_a] = {node_b: 1}
                graph[node_b] = {node_a: 1}
            elif node_b in graph[node_a]:
                graph[node_a][node_b] += 1
                graph[node_b][node_a] += 1
            else:
                graph[node_a][node_b] = 1
                graph[node_b][node_a] = 1


def create_es_bulk(graph):
    print("creating bulk file")
    max_size = int(9.5 * 1024 * 1024)
    file_count = 1
    output = StringIO()
    for hashtag in graph:
        obj = {'name': hashtag, 'neighbours': graph[hashtag]}
        output.write('{ "index" : { "_index" : "hashtags", "_type" : "hashtag" } }\n')
        output.write(json.dumps(obj))
        output.write('\n')
        if output.tell() > max_size and False:
            write_file(output, file_count)
            file_count += 1
            output.close()
            output = StringIO()

    write_file(output, file_count)


def reduce_neighbours(neighbours):
    heap = []
    for key in neighbours:
        heapq.heappush(heap, (neighbours[key], key))

    reduced = {}
    for t in heapq.nlargest(15, heap):
        reduced[t[1]] = t[0]

    return reduced


def minimize_graph(graph):
    print("minimizing graph")
    for hashtag in graph:
        if len(graph[hashtag]) > 15:
            graph[hashtag] = reduce_neighbours(graph[hashtag])


def print_timestamp():
    print(datetime.datetime.now().strftime("%H:%M:%S"))


def get_lines_count(file):
    fp = open(file, "r+")
    buffer = mmap.mmap(fp.fileno(), 0)
    line_count = 0
    while buffer.readline():
        line_count += 1

    fp.close()
    return line_count


def current_milli_time():
    return int(round(time.time() * 1000))


def main(file_name):
    print("python version: " + str(sys.version_info[0]) + "." + str(sys.version_info[1]) + "." + str(sys.version_info[2]))
    print_timestamp()
    graph = {}
    # graph = defaultdict(lambda: defaultdict(int))
    tags_count = get_lines_count(file_name)
    print("Number of records: %d" % tags_count)
    count = 0.0
    one_percent = int(tags_count / 100)
    cycle_start_time = current_milli_time()
    with open(file_name) as file:
        for line in file: #tqdm(file, total=get_lines_count(file_name)):
            if line.endswith('\n'):
                line = line[:-1]
            data = ast.literal_eval(line)
            add_to_graph(data, graph)
            # length = len(data)
            # for i in range(0, length):
            #     for j in range(i + 1, length):
            #         node_a = data[i]
            #         node_b = data[j]
            #         if node_a not in graph:
            #             graph[node_a] = {node_b: 1}
            #             graph[node_b] = {node_a: 1}
            #         elif node_b in graph[node_a]:
            #             graph[node_a][node_b] += 1
            #             graph[node_b][node_a] += 1
            #         else:
            #             graph[node_a][node_b] = 1
            #             graph[node_b][node_a] = 1
            count += 1
            if count % one_percent == 0:
                print('%.2f%%' % (count/tags_count * 100), end=' ', flush=True)
                # print(str(len(graph)) + " - size of graph")
                # print("avg calls for create_edge " + str(create_edge_counter / count))
                # print("time: " + str(current_milli_time() - cycle_start_time) + "ms\n")
                # cycle_start_time = current_milli_time()

    print("graph created, hashtags #" + str(len(graph)))
    minimize_graph(graph)
    create_es_bulk(graph)
    print_timestamp()


if __name__ == '__main__':
    main(sys.argv[1])
