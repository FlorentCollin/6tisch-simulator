import json
import seaborn as sns
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, List

sns.set_theme(style="whitegrid")

@dataclass
class Stats:
    sf_name: str
    motes_stats: Dict
    global_stats: Dict

def create_stats(stats_raw):
    motes_stats = {int(k): v for k, v in stats_raw['0'].items() if k.isdigit()}
    global_stats_key = 'global-stats'
    base_run_id = '0'
    global_stats = stats_raw['0'][global_stats_key]
    for run_id, run_stats in stats_raw.items():
        if run_id == base_run_id:
            continue
        mean_dicts(global_stats, run_stats[global_stats_key])
    sf_name = global_stats['sf_class']
    return Stats(sf_name, motes_stats, global_stats)

def mean_dicts(d1, d2):
    def helper(k, v, d2):
        if type(v) is dict:
            mean_dicts(v, d2)
            return v
        if type(v) is int:
            mean_value = (v + d2[k]) / 2
            d1[k] = mean_value
            return mean_value
        if type(v) is list:
            v2 = d2[k]
            for i in range(len(v)):
               v[i] = helper(k, v[i], v2[i]) 
            return v

    for k, v in d1.items():
        helper(k, v, d2)

def count_sixp_transactions(stats: Stats):
    count = 0
    for mote, mote_stats in stats.motes_stats.items():
        sixp_transactions = mote_stats['sixp_transactions_count']
        count += sixp_transactions[-1] if len(sixp_transactions) else 0
    return count

def barplot_sixp_transactions(stats_array: List[Stats]):
    sf_names = [stats.sf_name for stats in stats_array]
    sixp_transactions = [count_sixp_transactions(stats) for stats in stats_array]
    data = {
        "SF": sf_names,
        "#6P Transactions": sixp_transactions,
    }
    ax = sns.barplot(x='SF', y='#6P Transactions', data=data)
    ax.set_title('Number of 6P transactions completed by SF')
    for i in range(len(sixp_transactions)):
        ax.annotate(f'{sixp_transactions[i]}', xy=(i, sixp_transactions[i]), horizontalalignment='center')
    return ax

def extract_sixp_transactions_mote(stats: Stats, mote_id: int):
    mote_stats = stats.motes_stats[mote_id]
    return pd.DataFrame({
        'times': mote_stats['sixp_transactions_times'],
        'transactions_cumm': mote_stats['sixp_transactions_count']
    })

def plot_sixp_transactions(stats_array: List[Stats], mote_id: int):
    it = ((stats.sf_name, extract_sixp_transactions_mote(stats, mote_id)) for stats in stats_array)
    dfs = [df.assign(SF=sf_name) for sf_name, df in it]
    data = pd.concat(dfs)
    ax = sns.lineplot(data=data, x='times', y='transactions_cumm', hue='SF')
    ax.set_title(f'Cummulative 6P transactions count for mote {mote_id}')
    return ax

def extract_max_scheduled_cells(stats: Stats):
    count = 0
    for mote, mote_stats in stats.motes_stats.items():
        scheduled_cells = mote_stats['scheduled_cells_count']
        count += max(scheduled_cells) if len(scheduled_cells) else 0
    return count

def barplot_max_scheduled_cells(stats_array: List[Stats]):
    sf_names = [stats.sf_name for stats in stats_array]
    max_scheduled_cells = [extract_max_scheduled_cells(stats) for stats in stats_array]
    data = {
        "SF": sf_names,
        "max_scheduled_cells": max_scheduled_cells,
    }
    ax = sns.barplot(x='SF', y='max_scheduled_cells', data=data)
    ax.set_title('Maximum number of scheduled cells by SF')
    for i in range(len(max_scheduled_cells)):
        ax.annotate(f'{max_scheduled_cells[i]}', xy=(i, max_scheduled_cells[i]), horizontalalignment='center')
    return ax

def extract_scheduled_cells_mote(stats: Stats, mote_id: int):
    mote_stats = stats.motes_stats[mote_id]
    scheduled_cells_times = mote_stats['scheduled_cells_times']
    scheduled_cells_count = mote_stats['scheduled_cells_count']
    return pd.DataFrame({
        'times': scheduled_cells_times,
        'scheduled_cells': scheduled_cells_count,
    })

def plot_scheduled_cells_mote(stats_array: List[Stats], mote_id: int):
    it = ((stats.sf_name, extract_scheduled_cells_mote(stats, mote_id)) for stats in stats_array)
    dfs = [df.assign(SF=sf_name) for sf_name, df in it]
    data = pd.concat(dfs)
    ax = sns.scatterplot(data=data, x='times', y='scheduled_cells', hue='SF')
    ax.set_title(f'Scheduled Cells by SF on Mote {mote_id}')
    return ax

def extract_tx_queue_mote(stats: Stats, mote_id: int):
    mote_stats = stats.motes_stats[mote_id]
    scheduled_cells_times = mote_stats['tx_queue_times']
    scheduled_cells_count = mote_stats['tx_queue_length']
    return pd.DataFrame({
        'times': scheduled_cells_times,
        'tx_queue_length': scheduled_cells_count,
    })

def plot_tx_queue_mote(stats_array: List[Stats], mote_id: int):
    it = ((stats.sf_name, extract_tx_queue_mote(stats, mote_id)) for stats in stats_array)
    dfs = [df.assign(SF=sf_name) for sf_name, df in it]
    data = pd.concat(dfs)
    ax = sns.lineplot(data=data, x='times', y='tx_queue_length', hue='SF')
    ax.set_title(f'Length of TX queue by SF on Mote {mote_id}')
    return ax

def barplot_e2e_pdr(stats_array: List[Stats]):
    sf_names = [stats.sf_name for stats in stats_array]
    e2e_pdr  = [stats.global_stats['e2e-upstream-delivery'][0]['value'] for stats in stats_array]
    data = {
        "SF": sf_names,
        "E2E-PDR": e2e_pdr,
    }
    ax = sns.barplot(x='SF', y='E2E-PDR', data=data)
    ax.set_title('E2E Upstream Delivery Ratio')
    for i in range(len(e2e_pdr)):
        ax.annotate(f'{round(e2e_pdr[i] * 100, 2)}%', xy=(i, e2e_pdr[i] - 0.08), c='w', horizontalalignment='center')
    return ax

def barplot_e2e_latency(stats_array: List[Stats]):
    sf_names = [stats.sf_name for stats in stats_array]
    e2e_latency = [stats.global_stats['e2e-upstream-latency'][0]['95%'] for stats in stats_array]
    data = {
        "SF": sf_names,
        "E2E-Latency": e2e_latency,
    }
    ax = sns.barplot(x='E2E-Latency', y='SF', data=data)
    ax.set_title('E2E Upstream Latency 95% (s)')
    for i in range(len(e2e_latency)):
        ax.annotate(f'{round(e2e_latency[i], 2)} s', xy=(e2e_latency[i], i), verticalalignment='center')
    return ax

def barplot_joining_time(stats_array: List[Stats]):
    sf_names = [stats.sf_name for stats in stats_array]
    joining_time = [stats.global_stats['joining-time'][1]['max'] / 60 for stats in stats_array]
    data = {
        "SF": sf_names,
        "Joining time": joining_time,
    }
    ax = sns.barplot(x='Joining time', y='SF', data=data)
    ax.set_title('Maximum Joining Time (s)')
    for i in range(len(joining_time)):
        ax.annotate(f'{round(joining_time[i], 2)} m', xy=(joining_time[i], i), verticalalignment='center')
    return ax
    

def load_stats_from_filepath(filepath):
    with open(filepath) as f:
        return json.load(f)

def load_stats(sf_names: List[str]):
    log_dir_path = 'simData'
    num_motes = 7 # @incomplete
    data_files = (f'{log_dir_path}/{sf_name}/exec_numMotes_{num_motes}.dat.kpi' for sf_name in sf_names)
    stats_raw = (load_stats_from_filepath(filepath) for filepath in data_files)
    return [create_stats(x) for x in stats_raw]

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    sf_names = ["MSF", "OTF"]
    stats = load_stats(sf_names)
    # barplot_e2e_latency(stats)
    # plt.show()

    plt.figure()
    plot_scheduled_cells_mote(stats, 1)
    plt.show()