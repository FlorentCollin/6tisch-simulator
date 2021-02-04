#!/bin/sh
LOG_SETTINGS="simulator.state\nsimulator.random_seed\npacket_dropped\nno_route\ntxqueue_full\nno_tx_cells\nmax_retries\nreassembly_buffer_full\nvrb_table_full\ntime_exceeded\nrank_error\napp.tx\napp.rx\nsecjoin.tx\nsecjoin.rx\nsecjoin.joined\nsecjoin.unjoined\nsecjoin.failed\nrpl.dio.tx\nrpl.dio.rx\nrpl.dao.tx\nrpl.dao.rx\nrpl.dis.tx\nrpl.dis.rx\nrpl.churn\nrpl.local_repair\nsixlowpan.pkt.tx\nsixlowpan.pkt.fwd\nsixlowpan.pkt.rx\nsixlowpan.frag.gen\nmsf.tx_cell_utilization\nmsf.rx_cell_utilization\nmsf.error.schedule_full\nsixp.tx\nsixp.rx\nsixp.comp\nsixp.timeout\nsixp.abort\ntsch.synced\ntsch.desynced\ntsch.eb.tx\ntsch.eb.rx\ntsch.add_cell\ntsch.delete_cell\ntsch.txdone\ntsch.rxdone\ntsch.be.updated\ntsch.add_slotframe\ntsch.delete_slotframe\nradio.stats\nmac.add_addr\nipv6.add_addr\nprop.transmission\nprop.interference\nprop.drop_lockon\nconn.matrix.update"

echo -e $LOG_SETTINGS | fzf -m | xargs python -c "import sys; print(sys.argv[1:])" | sed "s/'/\"/g"
