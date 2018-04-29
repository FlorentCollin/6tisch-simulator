"""
Tests for SimEngine.Connectivity
"""

#============================ helpers =========================================

def print_matrix(matrix):
    # header
    print "\t",
    for source in matrix:
        print "{0}\t|".format(source),
    print "\n"
    for source in matrix:
        print "{0}\t|".format(source),
        for dest in matrix:
            print "{0}\t|".format(matrix[source][dest][11]['pdr']),
        print "\n"

#============================ tests ===========================================

#=== verify the connectivity matrix for the 'linear' is expected

def test_linear_matrix(sim_engine):
    """ creates a static connectivity linear path
        0 <-- 1 <-- 2 <-- ... <-- num_motes
    """

    num_motes = 6
    engine = sim_engine(
        diff_config = {
            'exec_numMotes': num_motes,
            'conn_type':     'linear',
        }
    )
    motes  = engine.motes
    matrix = engine.connectivity.connectivity_matrix

    print_matrix(matrix)

    assert motes[0].dagRoot is True

    for c in range(0, num_motes):
        for p in range(0, num_motes):
            if (c == p+1) or (c+1 == p):
                for ch in range(engine.settings.phy_numChans):
                    assert matrix[c][p][ch]['pdr']  ==  1.00
                    assert matrix[c][p][ch]['rssi'] ==   -10
            else:
                for ch in range(engine.settings.phy_numChans):
                    assert matrix[c][p][ch]['pdr']  == None
                    assert matrix[c][p][ch]['rssi'] == None

#=== verify propagate function doesn't raise exception

def test_propagate(sim_engine):
    engine = sim_engine()
    engine.connectivity.propagate()
