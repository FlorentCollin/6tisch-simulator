from __future__ import division
from __future__ import print_function

# =========================== adjust path =====================================

import os
import sys

import netaddr

if __name__ == '__main__':
    here = sys.path[0]
    sys.path.insert(0, os.path.join(here, '..'))

# ========================== imports ==========================================

import json
import glob
import numpy as np

from SimEngine import SimLog
import SimEngine.Mote.MoteDefines as d

# =========================== defines =========================================

DAGROOT_ID = 0  # we assume first mote is DAGRoot
DAGROOT_IP = 'fd00::1:0'

# =========================== decorators ======================================

def openfile(func):
    def inner(inputfile):
        with open(inputfile, 'r') as f:
            return func(f)
    return inner

# =========================== helpers =========================================

def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)

def init_mote():
    return {
        'upstream_num_tx': 0,
        'upstream_num_rx': 0,
        'upstream_num_lost': 0,
        'join_asn': None,
        'join_time_s': None,
        'sync_asn': None,
        'sync_time_s': None,
        'upstream_pkts': {},
        'latencies': [],
        'tx_queue_times': [],
        'tx_queue_length': [],
        'scheduled_cells_times': [],
        'scheduled_cells_count': [],
        'sixp_transactions_times': [],
        'sixp_transactions_count': [],
        'hops': [],
    }

# =========================== KPIs ============================================

@openfile
def kpis_all(inputfile):

    allstats = {} # indexed by run_id, mote_id

    file_settings = json.loads(inputfile.readline())  # first line contains settings

    # === gather raw stats

    for line in inputfile:
        logline = json.loads(line)

        # shorthands
        run_id = logline['_run_id']
        if '_asn' in logline: # TODO this should be enforced in each line
            asn = logline['_asn']
        if '_mote_id' in logline: # TODO this should be enforced in each line
            mote_id = logline['_mote_id']

        # populate
        if run_id not in allstats:
            allstats[run_id] = {}
        if (
                ('_mote_id' in logline)
                and
                (mote_id not in allstats[run_id])
                and
                (mote_id != DAGROOT_ID)
            ):
            allstats[run_id][mote_id] = init_mote()

        if   logline['_type'] == SimLog.LOG_TSCH_SYNCED['type']:
            # sync'ed

            # shorthands
            mote_id    = logline['_mote_id']

            # only log non-dagRoot sync times
            if mote_id == DAGROOT_ID:
                continue

            allstats[run_id][mote_id]['sync_asn']  = asn
            allstats[run_id][mote_id]['sync_time_s'] = asn*file_settings['tsch_slotDuration']

        elif logline['_type'] == SimLog.LOG_SECJOIN_JOINED['type']:
            # joined

            # shorthands
            mote_id    = logline['_mote_id']

            # only log non-dagRoot join times
            if mote_id == DAGROOT_ID:
                continue

            # populate
            assert allstats[run_id][mote_id]['sync_asn'] is not None
            allstats[run_id][mote_id]['join_asn']  = asn
            allstats[run_id][mote_id]['join_time_s'] = asn*file_settings['tsch_slotDuration']

        elif logline['_type'] == SimLog.LOG_APP_TX['type']:
            # packet transmission

            # shorthands
            mote_id    = logline['_mote_id']
            dstIp      = logline['packet']['net']['dstIp']
            appcounter = logline['packet']['app']['appcounter']

            # only log upstream packets
            if dstIp != DAGROOT_IP:
                continue

            # populate
            assert allstats[run_id][mote_id]['join_asn'] is not None
            if appcounter not in allstats[run_id][mote_id]['upstream_pkts']:
                allstats[run_id][mote_id]['upstream_pkts'][appcounter] = {
                    'hops': 0,
                }

            allstats[run_id][mote_id]['upstream_pkts'][appcounter]['tx_asn'] = asn

        elif logline['_type'] == SimLog.LOG_APP_RX['type']:
            # packet reception

            # shorthands
            mote_id    = netaddr.IPAddress(logline['packet']['net']['srcIp']).words[-1]
            dstIp      = logline['packet']['net']['dstIp']
            hop_limit  = logline['packet']['net']['hop_limit']
            appcounter = logline['packet']['app']['appcounter']

            # only log upstream packets
            if dstIp != DAGROOT_IP:
                continue

            allstats[run_id][mote_id]['upstream_pkts'][appcounter]['hops']   = (
                d.IPV6_DEFAULT_HOP_LIMIT - hop_limit + 1
            )
            allstats[run_id][mote_id]['upstream_pkts'][appcounter]['rx_asn'] = asn

        elif (logline['_type'] in (SimLog.LOG_TSCH_ADD_CELL['type'], SimLog.LOG_TSCH_DELETE_CELL['type'])
              and logline['slotFrameHandle'] == 2
              and mote_id != DAGROOT_ID):
            scheduled_cells_times = allstats[run_id][mote_id]['scheduled_cells_times']
            scheduled_cells_count = allstats[run_id][mote_id]['scheduled_cells_count']
            delta = 1 if logline['_type'] == SimLog.LOG_TSCH_ADD_CELL['type'] else -1
            count_transactions = delta if len(scheduled_cells_count) == 0 else scheduled_cells_count[-1] + delta
            time_s = asn * file_settings['tsch_slotDuration']
            scheduled_cells_times.append(time_s)
            scheduled_cells_count.append(count_transactions)

        elif logline['_type'] == SimLog.LOG_SIXP_TRANSACTION_COMPLETED['type'] and mote_id != DAGROOT_ID:
            sixp_transactions_times = allstats[run_id][mote_id]['sixp_transactions_times']
            sixp_transactions_count = allstats[run_id][mote_id]['sixp_transactions_count']
            count_transactions = 1 if len(sixp_transactions_count) == 0 else sixp_transactions_count[-1] + 1
            time_s = asn * file_settings['tsch_slotDuration']
            sixp_transactions_times.append(time_s)
            sixp_transactions_count.append(count_transactions)

        elif logline['_type'] == SimLog.LOG_TSCH_TXQUEUE_LENGTH['type'] and mote_id != DAGROOT_ID:
            tx_queue_times  = allstats[run_id][mote_id]['tx_queue_times']
            tx_queue_length = allstats[run_id][mote_id]['tx_queue_length']
            time_s = asn * file_settings['tsch_slotDuration']
            tx_queue_times.append(time_s)
            tx_queue_length.append(int(logline['length']))

        # elif logline['_type'] == SimLog.LOG_PACKET_DROPPED['type']:
            # mote_id = logline['_mote_id']
            # reason = logline['reason']
            # key = 'packet_dropped_reason'
            # if key in allstats[run_id][mote_id]:
                # allstats[run_id][mote_id][key].append(reason)
            # else:
                # allstats[run_id][mote_id][key] = [reason]
            

    # === compute advanced motestats

    for (run_id, per_mote_stats) in list(allstats.items()):
        for (mote_id, motestats) in list(per_mote_stats.items()):
            if mote_id != 0:
                if motestats['join_asn'] is not None:
                    # latencies, upstream_num_tx, upstream_num_rx, upstream_num_lost
                    for (appcounter, pktstats) in list(allstats[run_id][mote_id]['upstream_pkts'].items()):
                        motestats['upstream_num_tx']      += 1
                        if 'rx_asn' in pktstats:
                            motestats['upstream_num_rx']  += 1
                            thislatency = (pktstats['rx_asn']-pktstats['tx_asn'])*file_settings['tsch_slotDuration']
                            motestats['latencies']  += [thislatency]
                            motestats['hops']       += [pktstats['hops']]
                        else:
                            motestats['upstream_num_lost'] += 1
                    if (motestats['upstream_num_rx'] > 0) and (motestats['upstream_num_tx'] > 0):
                        motestats['latency_min_s'] = min(motestats['latencies'])
                        motestats['latency_avg_s'] = sum(motestats['latencies'])/float(len(motestats['latencies']))
                        motestats['latency_max_s'] = max(motestats['latencies'])
                        motestats['upstream_reliability'] = motestats['upstream_num_rx']/float(motestats['upstream_num_tx'])
                        motestats['avg_hops'] = sum(motestats['hops'])/float(len(motestats['hops']))

    # === network stats
    for (run_id, per_mote_stats) in list(allstats.items()):

        #-- define stats

        app_packets_sent = 0
        app_packets_received = 0
        app_packets_lost = 0
        joining_times = []
        us_latencies = []
        sixp_transactions = []
        slot_duration = file_settings['tsch_slotDuration']

        #-- compute stats
        for (mote_id, motestats) in list(per_mote_stats.items()):
            if mote_id == DAGROOT_ID:
                continue

            # counters
            app_packets_sent += motestats['upstream_num_tx']
            app_packets_received += motestats['upstream_num_rx']
            app_packets_lost += motestats['upstream_num_lost']

            # joining times
            if motestats['join_asn'] is not None:
                joining_times.append(motestats['join_asn'])

            # latency
            us_latencies += motestats['latencies']

            # 6P transactions
            mote_sixp_transactions = motestats['sixp_transactions_count']
            n_sixp_transactions = mote_sixp_transactions[-1] if mote_sixp_transactions else 0
            sixp_transactions.append(n_sixp_transactions)

        #-- save stats
        allstats[run_id]['global-stats'] = {
            'sf_class': file_settings['sf_class'],
            'e2e-upstream-delivery': [
                {
                    'name': 'E2E Upstream Delivery Ratio',
                    'unit': '%',
                    'value': (
                        1 - app_packets_lost / app_packets_sent
                        if app_packets_sent > 0 else 'N/A'
                    )
                },
                {
                    'name': 'E2E Upstream Loss Rate',
                    'unit': '%',
                    'value': (
                        app_packets_lost / app_packets_sent
                        if app_packets_sent > 0 else 'N/A'
                    )
                }
            ],
            'e2e-upstream-latency': [
                {
                    'name': 'E2E Upstream Latency',
                    'unit': 's',
                    'mean': (
                        mean(us_latencies)
                        if us_latencies else 'N/A'
                    ),
                    'min': (
                        min(us_latencies)
                        if us_latencies else 'N/A'
                    ),
                    'max': (
                        max(us_latencies)
                        if us_latencies else 'N/A'
                    ),
                    '99%': (
                        np.percentile(us_latencies, 99)
                        if us_latencies else 'N/A'
                    ),
                    '95%': (
                        np.percentile(us_latencies, 95)
                        if us_latencies else 'N/A'
                    )
                },
                {
                    'name': 'E2E Upstream Latency',
                    'unit': 'slots',
                    'mean': (
                        mean(us_latencies) / slot_duration
                        if us_latencies else 'N/A'
                    ),
                    'min': (
                        min(us_latencies) / slot_duration
                        if us_latencies else 'N/A'
                    ),
                    'max': (
                        max(us_latencies) / slot_duration
                        if us_latencies else 'N/A'
                    ),
                    '99%': (
                        np.percentile(us_latencies, 99) / slot_duration
                        if us_latencies else 'N/A'
                    ),
                    '95%': (
                        np.percentile(us_latencies, 95) / slot_duration
                        if us_latencies else 'N/A'
                    )
                }
            ],
            'joining-time': [
                {
                    'name': 'Joining Time',
                    'unit': 'slots',
                    'min': (
                        min(joining_times)
                        if joining_times else 'N/A'
                    ),
                    'max': (
                        max(joining_times)
                        if joining_times else 'N/A'
                    ),
                    'mean': (
                        mean(joining_times)
                        if joining_times else 'N/A'
                    ),
                    '99%': (
                        np.percentile(joining_times, 99)
                        if joining_times else 'N/A'
                    )
                },
                {
                    'name': 'Joining Time',
                    'unit': 's',
                    'min': (
                        min(joining_times) * slot_duration
                        if joining_times else 'N/A'
                    ),
                    'max': (
                        max(joining_times) * slot_duration
                        if joining_times else 'N/A'
                    ),
                    'mean': (
                        mean(joining_times) * slot_duration
                        if joining_times else 'N/A'
                    ),
                    '99%': (
                        np.percentile(joining_times, 99) * slot_duration
                        if joining_times else 'N/A'
                    )
                }
            ],
            'app-packets-sent': [
                {
                    'name': 'Number of application packets sent',
                    'total': app_packets_sent
                }
            ],
            'app_packets_received': [
                {
                    'name': 'Number of application packets received',
                    'total': app_packets_received
                }
            ],
            'app_packets_lost': [
                {
                    'name': 'Number of application packets lost',
                    'total': app_packets_lost
                }
            ],
            'sixp-transactions': [
                {
                    'name': 'Number of 6P transactions completed',
                    'total': sum(sixp_transactions) if sixp_transactions else 'N/A'
                }
            ],
        }

    # === remove unnecessary stats
    for (run_id, per_mote_stats) in list(allstats.items()):
        for (mote_id, motestats) in list(per_mote_stats.items()):
            if 'sync_asn' in motestats:
                del motestats['sync_asn']
            if 'join_asn' in motestats:
                del motestats['upstream_pkts']
                del motestats['hops']
                del motestats['join_asn']
            if 'latencies' in motestats:
                del motestats['latencies']

    return allstats

# =========================== main ============================================

def main():

    # FIXME: This logic could be a helper method for other scripts
    # Identify simData having the latest results. That directory should have
    # the latest "mtime".
    subfolders = list(
        [os.path.join('simData', x) for x in os.listdir('simData')]
    )
    subfolder = max(subfolders, key=os.path.getmtime)
    for infile in glob.glob(os.path.join(subfolder, '*.dat')):
        print('generating KPIs for {0}'.format(infile))

        # gather the kpis
        kpis = kpis_all(infile)

        # print on the terminal
        print(json.dumps(kpis, indent=4))

        # add to the data folder
        outfile = '{0}.kpi'.format(infile)
        with open(outfile, 'w') as f:
            f.write(json.dumps(kpis, indent=4))
        print('KPIs saved in {0}'.format(outfile))

if __name__ == '__main__':
    main()
