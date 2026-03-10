from gitplot.git import sample_evenly


def test_sample_evenly_fewer_than_n():
    commits = [("a", 1), ("b", 2), ("c", 3)]
    assert sample_evenly(commits, 10) == commits


def test_sample_evenly_includes_last():
    commits = [(str(i), i) for i in range(100)]
    sampled = sample_evenly(commits, 5)
    assert sampled[-1] == commits[-1]
    assert len(sampled) == 5
