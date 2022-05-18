from experiment import run_experiment
from timeit import default_timer as timer


def run_benchmark():
    benchmark_file = open('benchmark_res.txt', 'w')
    for n_threads in [1, 2, 5, 10, 50, 100, 500, 1000]:
        start = timer()
        print(f'{n_threads} worker(s):')
        benchmark_file.write(f'{n_threads} workers:\n')
        res = run_experiment(
            n_threads=n_threads,
            dest_dir='images',
            links_filepath='links.txt'
        )
        end = timer() - start
        benchmark_file.write(f"Avg times: {str(res)}\nOverall: {round(end, 3)}s \n\n")
    benchmark_file.close()


if __name__ == "__main__":
    run_benchmark()
