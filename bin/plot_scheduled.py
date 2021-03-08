import argparse
import json
import matplotlib.pyplot as plt

def main():
    parser = argparse.ArgumentParser(description='Plot number of scheduled cells for motes')
    parser.add_argument('-m', '--mote', type=str, help="mote's id")
    parser.add_argument('file', nargs=1)
    args = parser.parse_args()

    with open(args.file[0], 'r') as json_file:
        data = json.load(json_file)
        # we assume only one run -- TODO: handle multiples runs
        motes = {k: v for k, v in data['0'].items() if k.isdigit()}
        if args.mote:
            # Mote ID should be valid
            assert(args.mote in motes)
            motes = {k: v for k, v in motes.items() if k == args.mote}
        for mote_id, mote_stats in motes.items():
            scheduled_cells = mote_stats['scheduled_cells']
            times_min = list(t['time_s'] / 60 for t in scheduled_cells)
            num_scheduled_cells = list(t['num_scheduled_cells'] for t in scheduled_cells)
            plot(num_scheduled_cells, times_min, mote_id)

def plot(xs, ys, mote_id):
    plt.plot(ys, xs, '.')
    plt.ylabel("Number of Scheduled Cells")
    plt.xlabel("Time(min)")
    plt.title(f"Scheduled Cells - Mote {mote_id}")
    plt.show()
    plt.clf()


if __name__ == '__main__':
    main()
