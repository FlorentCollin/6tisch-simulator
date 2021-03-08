import argparse
import json
import matplotlib.pyplot as plt

def main():
    parser = argparse.ArgumentParser(description='Plot number of sixp transactions completed for a mote')
    parser.add_argument('-m', '--mote', type=str, required=True, help="mote's id")
    parser.add_argument('file', nargs=1)
    args = parser.parse_args()

    with open(args.file[0], 'r') as json_file:
        data = json.load(json_file)
        # we assume only one run -- TODO: handle multiples runs
        # filter only mote stats and discard global stats
        motes = {k: v for k, v in data['0'].items() if k.isdigit()}
        # Mote ID should be valid
        assert(args.mote in motes)
        mote_stats = motes[args.mote]
        sixp_transactions = mote_stats['sixp_transactions']
        times_min = list(t['time_s'] / 60 for t in sixp_transactions)
        transactions_count = list(t['count'] for t in sixp_transactions)
        plot(transactions_count, times_min, args.mote)

def plot(xs, ys, mote_id):
    plt.plot(ys, xs, '.')
    plt.ylabel("Transactions Completed")
    plt.xlabel("Time(m)")
    plt.title(f"Number of 6P transactions completed for Mote: {mote_id}")
    plt.show()
    plt.clf()


if __name__ == '__main__':
    main()
