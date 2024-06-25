from collections import *
from decimal import Decimal
from heapq import *
from bisect import *
from itertools import *
from math import *
from functools import *
import os
import sys

if os.path.exists('input.txt'):
    sys.stdin = open("input.txt", "r")
    sys.stdout = open("output.txt", "w")


def db(ip):
    if os.path.exists("output.txt"):
        print(ip)


def strin(): return sys.stdin.readline().strip()
def arrin(): return list(map(int, sys.stdin.readline().split()))
def intin(): return int(sys.stdin.readline())


sys.setrecursionlimit(10 ** 7)


# ----------------------------------------------------------------------------
def solve():







# ------------------------------------------------------------------------------
# t = int(sys.stdin.readline())
t = 1
for _ in range(t):
    try:
        print(solve())
    except Exception as e:
        print(e)
sys.stdout.close()
sys.stdin.close()