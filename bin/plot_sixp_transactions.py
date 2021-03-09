import argparse
import json
import matplotlib.pyplot as plt
import math

def main():
    parser = argparse.ArgumentParser(description='Plot number of sixp transactions completed for a mote')
    parser.add_argument('file', nargs=1)
    args = parser.parse_args()

    with open(args.file[0], 'r') as json_file:
        data = json.load(json_file)
        # we assume only one run -- TODO: handle multiples runs
        # filter only mote stats and discard global stats
        motes = {k: v for k, v in data['0'].items() if k.isdigit()}
        # Mote ID should be valid
        n = math.ceil(math.sqrt(len(motes)))
        fig, axis = plt.subplots(n, n, figsize=(12.8, 7.2))
        fig.tight_layout(h_pad=4)
        fig.suptitle("Number of 6P transactions completed for each mote")
        plt.subplots_adjust(top=0.90)

        for i, mote in enumerate(motes):
            mote_stats = motes[mote]
            sixp_transactions = mote_stats['sixp_transactions']
            times_min = list(t['time_s'] / 60 for t in sixp_transactions)
            transactions_count = list(t['count'] for t in sixp_transactions)
            row, col = i // n, i % n
            axis[row, col].plot(times_min, transactions_count, '.')
            axis[row, col].set_ylabel("Transactions Completed")
            axis[row, col].set_xlabel("Time(m)")
            axis[row, col].set_title(f"Mote {mote}")
        for i in range(len(motes), n**2):
            axis[i // n, i % n].set_visible(False)
        plt.show()


if __name__ == '__main__':
    main()
