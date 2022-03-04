# -*- coding: utf-8 -*-
"""
Created on Wed Mar 2 12:19:51 2022

@author: 13104

compare chainlink with v2 and v3 uniswap for time period since may 6th, 2021. Using ETH/USDC

add link to article in jupyter notebook on how to work with storage data.
"""

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns

# used for TWAP time framing on uniswap data. Set it to <None> if you want the fastest TWAP.
min_floor = '5min' #None

"""
uniswap v2 prices (done)
"""

uni_v2 = pd.read_csv(r'oracle_prices/uniswap_v2_price.csv')
uni_v2["datetime"] = pd.to_datetime(uni_v2["TIMESTAMP"])

if min_floor != None:
    uni_v2["minute_floor"] = uni_v2['datetime'].dt.floor(min_floor) #adjust this if you want to have different subsets.
    uni_v2.drop_duplicates(subset=["minute_floor"], keep='last', inplace=True) #keep only last swap results from a block/timestamp, in case of multi-swap
else:
    uni_v2.drop_duplicates(subset=["TIMESTAMP"], keep='last', inplace=True) #keep only last swap results from a block/timestamp, in case of multi-swap
uni_v2 = uni_v2.sort_values(by='datetime', ascending=True)
uni_v2.reset_index(drop=True, inplace=True)

uni_v2["price1Diff"] = uni_v2["CUMULATIVE_LAST"].diff(1)
uni_v2["price1Diff"] = uni_v2["price1Diff"].div(2**112) #decode UQ112x112 used when storing prices in _update() function
uni_v2["timeDiffSeconds"] = uni_v2["datetime"].diff(1)
uni_v2["timeDiffSeconds"] = uni_v2["timeDiffSeconds"].apply(lambda x: x.total_seconds())
uni_v2["uni_v2_ETH_price"] = uni_v2["price1Diff"].div(uni_v2["timeDiffSeconds"])
uni_v2["uni_v2_ETH_price"] = uni_v2["uni_v2_ETH_price"].apply(lambda x: x * 1e12) #moving decimals

uni_v2 = uni_v2[(uni_v2['uni_v2_ETH_price'] > 1000) & (uni_v2['uni_v2_ETH_price'] < 10000)] #some tx_hashes in EDW have wrong timestamp rn.
# uni_v2.plot(kind = "line", x = "datetime", y = "uni_v2_ETH_price")

"""
uniswap v3 prices (done)
"""
uni_v3_03 = pd.read_csv(r'oracle_prices/uniswapv3_storage_003_0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8.csv')
uni_v3_03.dropna(inplace=True)
uni_v3_03["datetime"] = pd.to_datetime(uni_v3_03["TIMESTAMP"])

if min_floor != None:
    uni_v3_03["minute_floor"] = uni_v3_03['datetime'].dt.floor(min_floor) #adjust this if you want to have different subsets.
    uni_v3_03.drop_duplicates(subset=["minute_floor"], keep='last', inplace=True) #keep only last swap results from a block/timestamp, in case of multi-swap
else:
    uni_v3_03.drop_duplicates(subset=["TIMESTAMP"], keep='last', inplace=True)
uni_v3_03.reset_index(drop=True, inplace=True)

uni_v3_03["tickDiff"] = uni_v3_03["TICKCUMULATIVE"].diff(1)
uni_v3_03["timeDiffSeconds"] = uni_v3_03["datetime"].diff(1)
uni_v3_03["timeDiffSeconds"] = uni_v3_03["timeDiffSeconds"].apply(lambda x: x.total_seconds())
uni_v3_03["average_tick"] = uni_v3_03["tickDiff"].div(uni_v3_03["timeDiffSeconds"])
uni_v3_03["uni_v3_ETH_price"] = uni_v3_03["average_tick"].apply(lambda x: 1/(1.0001**x) * 1e12) #inverse of exponential for token1, then move decimals

uni_v3_03 = uni_v3_03[(uni_v3_03['average_tick'] > 100000) & (uni_v3_03['average_tick'] < 300000)] #20 or so occassionally weird tick values below 100k. not sure why yet.
# uni_v3_03.plot(kind = "line", x = "datetime", y = "uni_v3_ETH_price")

"""
chainlink prices (done)

we include all unique times just to make joins cleaner and getting oracle diffs more reliable later.
"""
chainlink = pd.read_csv(r'oracle_prices/chainlink_oracle_price.csv')
chainlink["datetime"] = pd.to_datetime(chainlink['TIMESTAMP'])
chainlink = chainlink[chainlink["ETH_PRICE"] != 0] #for some reason EDW shows some 70 values as 0, maybe a read error?
chainlink.drop_duplicates(subset='TIMESTAMP', keep='last', inplace=True)
chainlink.rename(columns={'ETH_PRICE': 'chainlink_ETH_price'}, inplace=True)
# chainlink.plot(kind="line", x='datetime', y = 'chainlink_ETH_price')

"""joining all and plotting"""
notionals = ["chainlink_ETH_price", "uni_v2_ETH_price", "uni_v3_ETH_price"] 
comps = ["univ3_chainlink_diff", "univ2_chainlink_diff", "univ2_chainlink_ratio","univ3_chainlink_ratio"]

merged_data = pd.merge(uni_v2[["datetime", "uni_v2_ETH_price"]], uni_v3_03[["datetime", "uni_v3_ETH_price"]],left_on = 'datetime', right_on = 'datetime', how = 'outer')
merged_data = pd.merge(merged_data, chainlink[["datetime", "chainlink_ETH_price"]], left_on = 'datetime', right_on = 'datetime', how = 'outer')
merged_data.sort_values(by = 'datetime', ascending=True, inplace=True)
merged_data = merged_data.ffill() #chainlink ffill is a bit erroneous but good enough for now.

merged_data["univ2_chainlink_diff"] = merged_data["uni_v2_ETH_price"] - merged_data["chainlink_ETH_price"]
merged_data["univ3_chainlink_diff"] = merged_data["uni_v3_ETH_price"] - merged_data["chainlink_ETH_price"]
merged_data["univ2_chainlink_ratio"] = merged_data["uni_v2_ETH_price"].div(merged_data["chainlink_ETH_price"])
merged_data["univ3_chainlink_ratio"] = merged_data["uni_v3_ETH_price"].div(merged_data["chainlink_ETH_price"])

merged_data["minute"] = merged_data["datetime"].dt.round('1min')
minute_differences = merged_data.pivot_table(index="minute", values = comps, aggfunc = "mean")
minute_differences.reset_index(inplace=True)

## line plot notionals
merged_data.plot(kind="line", 
                  x = "datetime", 
                  y = notionals,
                  title = "Uniswap TWAP vs Chainlink Agg ETH/USDC Price Oracles: Price Notionals",
                  subplots=True,
                  figsize = (10,10))

## line plot comps
minute_differences.plot(kind="line", 
                  x = "minute", 
                  y = comps, 
                  subplots = True, 
                  figsize = (10,10),
                  title = "Uniswap TWAP vs Chainlink Agg ETH/USDC Price Oracles: Price Diff/Ratios")

###scatterplot, not best way of presenting. should show std devs instead.
# vs = ['v2', 'v3']
# for i, v in enumerate(vs):
#     f, ax = plt.subplots(figsize=(10, 10))
#     sns.despine(f, left=True, bottom=True)
#     sns.scatterplot(x="minute", y=f"uni{v}_chainlink_diff",
#                     linewidth=0,
#                     size = 0.1,
#                     alpha = 0.2,
#                     data=minute_differences, 
#                     ax=ax,
#                     legend=False)
#     ax.set(title = f"Uniswap {v} versus Chainlink ETH/USDC Prices (Minute)")

#stacked dfs for seaborn hue stuff (requires typed cloumns)
differences_df = minute_differences[["minute","univ3_chainlink_diff", "univ2_chainlink_diff"]].set_index("minute").stack()
differences_df = differences_df.reset_index()
differences_df.columns = ["minute", "price_source", "price_difference"]

ratio_df = minute_differences[["minute","univ3_chainlink_ratio", "univ2_chainlink_ratio"]].set_index("minute").stack()
ratio_df = ratio_df.reset_index()
ratio_df.columns = ["minute", "price_source", "price_ratio"]

diff_fig = sns.kdeplot(data=differences_df, x="price_difference", hue="price_source", log_scale=True)
ratio_fig = sns.kdeplot(data=ratio_df, x="price_ratio", hue="price_source", log_scale=False)
"""
still a lot of additional analysis that could be done such as:
    - role of arb bots
    - zooming in on different phases/trends in the time series
    - studying liquidity depth at points in time
    - trading volume correlation with ratio/diffs at points in time
    - different token pairs (two non-stables would be fun)
    - different trading paths (i.e. ETH/USDC should be closer to 1 because its in the middle of many paths and is most active by volume)
"""

"""
Oracle Usage Analysis

add timeseries for number of reads
add timeseries for unique contract sources a day
add some commentary on top users of each oracle.
"""

time_univ3 = pd.read_csv(r'oracle_reads/daily_reads_uniswapv3.csv')
time_univ3.columns = ["datetime", "daily total oracle calls", "daily unique contracts (calling oracle)"]
time_univ3["datetime"] = pd.to_datetime(time_univ3['datetime'])

time_chainlink = pd.read_csv(r'oracle_reads/daily_reads_chainlink.csv')
time_chainlink.columns = ["datetime", "daily total oracle calls", "daily unique contracts (calling oracle)"]
time_chainlink["datetime"] = pd.to_datetime(time_chainlink['datetime'])

fig, (ax1, ax2) = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(10,15))
ax1twin = ax1.twinx()
ax2twin = ax2.twinx()

time_univ3.plot(kind="line", 
                    x = "datetime", 
                    y = "daily total oracle calls",
                    color = 'teal',
                    title = "Uniswap TWAP ETH/USDC: Daily total calls and unique contracts making calls",
                    # legend=False,
                    ax = ax1twin)

time_univ3.plot(kind="line", 
                    x = "datetime", 
                    y = "daily unique contracts (calling oracle)",
                    color = "darkblue",
                    legend=False,
                    ax = ax1)

ax1.set(ylabel="daily total oracle calls")
ax1.tick_params(axis='y', colors='teal')
ax1.yaxis.label.set_color('teal')
ax1twin.set(ylabel="daily unique contracts (calling oracle)")
ax1twin.tick_params(axis='y', colors='darkblue')
ax1twin.yaxis.label.set_color('darkblue')


time_chainlink.plot(kind="line", 
                    x = "datetime", 
                    y = "daily total oracle calls",
                    title = "Chainlink Data Feed ETH/USDC: Daily total calls and unique contracts making calls",
                    color = 'teal',
                    # legend=False,
                    ax = ax2twin)

time_chainlink.plot(kind="line", 
                    x = "datetime", 
                    y = "daily unique contracts (calling oracle)",
                    color = 'darkblue',
                    legend=False,
                    ax = ax2)

ax2.set(ylabel="daily total oracle calls")
ax2.tick_params(axis='y', colors='teal')
ax2.yaxis.label.set_color('teal')
ax2twin.set(ylabel="daily unique contracts (calling oracle)")
ax2twin.tick_params(axis='y', colors='darkblue')
ax2twin.yaxis.label.set_color('darkblue')

