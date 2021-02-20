from mpi4py import MPI
from random import randint
from math import floor


def seq_merge(A, B):
    """ Performs a sequential merge on two arrays """
    i, j = 0, 0
    C = []
    while i < len(A) and j < len(B):
        if A[i] < B[j]:
            C.append(A[i])
            i += 1
        else:
            C.append(B[j])
            j += 1
    while i < len(A):
        C.append(A[i])
        i += 1
    while j < len(B):
        C.append(B[j])
        j += 1
    return C


def main():
    comm = MPI.COMM_WORLD
    p = comm.Get_size()
    my_rank = comm.Get_rank()

    n = 1000000  # Array size

    if my_rank == 0:
        A = sorted([randint(1, 10000000) for _ in range(n)])
        B = sorted([randint(1, 10000000) for _ in range(n)])
        C = []

        print(A)
        print(B)

        j = []

        A_start = my_rank * floor(n / p) + min(my_rank, n % p)
        A_end = (my_rank + 1) * floor(n / p) + min((my_rank + 1), n % p)
        B_start = my_rank

        i = B_start
        while i < len(B):
            if B[i] > A[A_end - 1]:
                j.append(i)
                break
            i += 1
            if i == len(B):
                j.append(i)

        B_end = j[my_rank]

        C.extend(seq_merge(A[A_start:A_end], B[B_start:B_end]))

        end_of_B = False

        for dest in range(1, p):
            dest_A_start = dest * floor(n / p) + min(dest, n % p)
            dest_A_end = (dest + 1) * floor(n / p) + min((dest + 1), n % p)

            if end_of_B:
                dest_B_start = -1
                dest_B_end = -1

                comm.send((A[dest_A_start:dest_A_end], B[dest_B_start:dest_B_end]), dest=dest, tag=0)
                continue

            dest_B_start = j[dest - 1]

            i = dest_B_start
            while i < len(B):
                if B[i] > A[dest_A_end - 1]:
                    j.append(i)
                    break
                i += 1
                if i == len(B):
                    j.append(i)

            if dest == p - 1:
                dest_B_end = len(B)
            else:
                dest_B_end = j[dest]

            if dest_B_end == len(B):
                end_of_B = True

            comm.send((A[dest_A_start:dest_A_end], B[dest_B_start:dest_B_end]), dest=dest, tag=0)

        for source in range(1, p):
            inbound_data = comm.recv(source=source, tag=0)
            C.extend(inbound_data)

        print(C)

    else:
        inbound_data = comm.recv(source=0, tag=0)
        A, B = inbound_data[0], inbound_data[1]
        merge_result = seq_merge(A, B)
        comm.send(merge_result, dest=0, tag=0)


if __name__ == '__main__':
    main()
