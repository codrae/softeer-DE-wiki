from multiprocessing import Queue

items = [(1, 'red'), (2, 'green'), (3, 'blue'), (4, 'black')]

q = Queue()

if __name__ == '__main__':
    print("pushing items to queue:")
    for item in items:
        item_number, color = item
        print(f"item no : {item_number} {color}")
        q.put(item)

    print("popping items for queue:")
    #while not q.empty(): -> empty 믿을 수 없음.
    for _ in range(len(items)):
        item = q.get()
        item_number, color = item
        print(f"item no : {item_number} {color}")
