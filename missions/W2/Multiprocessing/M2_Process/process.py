from multiprocessing import Process

def print_continent(continent = "Asia"):
    print(f"The name of continent is : {continent}")

if __name__ == "__main__":
    continents = ["Asia", "America", "Europe", "Africa"]
    process = []

    for con in continents:
        p = Process(target=print_continent, args=(con,))
        process.append(p)
        p.start()

    for p in process:
        p.join()