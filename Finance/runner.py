import os
import sys
from dotenv import load_dotenv
import pandas as pd
import polars as pl

from Finance.bot import BOT
from Finance.orders import ORDERS

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

if os.path.exists('input.txt'):
    sys.stdin = open("input.txt", "r")
sys.stdout = open("output.txt", "w")

load_dotenv()

api_key = os.getenv("api_key")
secret_key = os.getenv("secret_key")

bot = BOT(api_key = api_key, secret_key = secret_key)
# pl.options.display.max_rows = None
# pl.options.display.max_columns = None

with pl.Config() as cfg:
    cfg.set_tbl_cols(-1)
    cfg.set_tbl_rows(-1)
    bot.show_orders()

    for i in range(10):
        print()

    bot.show_portfolio()