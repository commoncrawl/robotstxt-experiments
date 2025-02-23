"""
Combine multiple ranked lists to a single one using the Dowdall rule,
see https://en.wikipedia.org/wiki/Borda_count#Dowdall
"""

import fileinput
import sys


def combine_sorted_with_rank(input, sep: str = ','):
    """Fast combination of multiple ranked lists where ranks are given
    and the concatenated lists are sorted by values. The highest rank
    must be '1', the rank field is expected to be the first field in a
    line."""

    last_value = None
    combined_rank = 0

    for line in input:
        try:
            (rank, value) = line.rstrip('\r\n').split(sep, 1)
            if value == last_value:
                combined_rank += 1/int(rank)
            else:
                if last_value is not None:
                    print(combined_rank, last_value, sep=sep)
                combined_rank = 1/int(rank)
                last_value = value
        except Exception as e:
            sys.stderr.write('Failed to read line <' + line + '>: ' + str(e))
            exit(1)

    if last_value is not None:
        print(combined_rank, last_value, sep=sep)


if __name__ == "__main__":
    combine_sorted_with_rank(fileinput.input())
