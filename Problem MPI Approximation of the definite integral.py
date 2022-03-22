from mpi4py import MPI
import sys


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    a, b, N, r = accept()
    dest_rank = 0
    if rank == 0:
        assert N != 0
        length = (b - a) / N
        left = a
        right = a + length
        total = 0
        i = 0
        while i < N:
            intervals = []
            for j in range(r):
                # join r intervals to a block and send it to rank 1+
                intervals.append(
                    {
                        'a': left,
                        'b': right
                    }
                )
                left += length
                right += length
                i += 1
                if i >= N:
                    break
            data = {
                'function': f,
                'intervals': intervals,
                'finish': False
            }
            comm.send(data, dest=dest_rank % (size - 1) + 1, tag=11)
            # wait for finished calculation of previous block
            res = comm.recv(source=dest_rank % (size - 1) + 1, tag=11)
            dest_rank += 1
            total += res
        print(total, flush=True)
        for i in range(1, size):
            data = {
                'finish': True
            }
            comm.send(data, dest=i, tag=11)

    else:
        while True:
            data = comm.recv(source=0, tag=11)
            if data['finish']:
                break
            intervals = data['intervals']
            answer = 0
            for i in intervals:
                answer += area(data['function'], i['a'], i['b'])
            comm.send(answer, dest=0, tag=11)


def accept():
    a = int(sys.argv[1])
    b = int(sys.argv[2])
    N = int(sys.argv[3])
    r = int(sys.argv[4])
    return a, b, N, r


def f(x):
    y = 1.25 * x + 2
    return y


def area(f, a, b):
    ar = ((f(a) + f(b)) / 2) * (b - a)
    return ar


if __name__ == '__main__':
    main()