import importlib
from collections import defaultdict
from unittest import mock

import pytest

cli_mod = importlib.import_module("gwasstudio.cli.list")


# ----------------------------------------------------------------------
# Helper fixtures -------------------------------------------------------
# ----------------------------------------------------------------------
@pytest.fixture
def fake_cm():
    """
    A minimal stub for ConfigurationManager that only provides
    ``get_data_category_list``.
    """

    class FakeCM:
        # You can make this a property or a method – the original code
        # accesses it as an attribute, so we do the same.
        get_data_category_list = ["cat_A", "cat_B"]

    return FakeCM()


@pytest.fixture
def fake_profile():
    """
    A dummy EnhancedDataProfile – the real class is never used
    inside the helpers, we only need something to pass in.
    """

    class DummyProfile:
        def __init__(self, uri: str = "mongodb://test"):
            self.uri = uri

    return DummyProfile()


# ----------------------------------------------------------------------
# Tests for _collect_objects -------------------------------------------
# ----------------------------------------------------------------------
def test_collect_objects_calls_query_mongo_obj_correctly(fake_cm, fake_profile):
    """
    Verify that `_collect_objects` iterates over every category and forwards
    the right filter dict to `query_mongo_obj`.
    """
    # --- arrange -------------------------------------------------------
    # Prepare a side‑effect that records the incoming arguments.
    call_log = []

    def fake_query(filter_dict, profile):
        # Record what we got so we can assert later.
        call_log.append((filter_dict, profile))
        # Return a simple list that contains the filter dict for later
        # inspection – the content itself does not matter for this test.
        return [{"category": filter_dict["category"], "project": "P", "study": "S"}]

    # Patch the function *in the module where it is looked up*.
    with mock.patch.object(cli_mod, "query_mongo_obj", side_effect=fake_query):
        # --- act ---------------------------------------------------------
        result = cli_mod._collect_objects(fake_cm, fake_profile)

    # --- assert ---------------------------------------------------------
    # 1️⃣  `query_mongo_obj` must have been called once per category
    assert len(call_log) == len(fake_cm.get_data_category_list)
    expected_filters = [{"category": cat} for cat in fake_cm.get_data_category_list]
    assert [c[0] for c in call_log] == expected_filters

    # 2️⃣  The profile object passed through unchanged
    assert all(c[1] is fake_profile for c in call_log)

    # 3️⃣  The returned list is a flattened collection of the stubbed objects
    #    (2 categories → 2 dicts)
    assert isinstance(result, list)
    assert len(result) == len(fake_cm.get_data_category_list)
    assert all(isinstance(item, dict) for item in result)
    # each dict must contain the category we asked for
    returned_cats = {d["category"] for d in result}
    assert returned_cats == set(fake_cm.get_data_category_list)


def test_collect_objects_when_no_categories(fake_profile):
    """
    Edge case – the configuration manager returns an empty list.
    The function should simply return an empty list and never call
    `query_mongo_obj`.
    """

    class EmptyCM:
        get_data_category_list = []

    with mock.patch.object(cli_mod, "query_mongo_obj") as mocked_q:
        result = cli_mod._collect_objects(EmptyCM(), fake_profile)

    # No DB calls
    mocked_q.assert_not_called()
    # Result is empty
    assert result == []


# ----------------------------------------------------------------------
# Tests for _build_category_map -----------------------------------------
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "input_objs,expected_map",
    [
        # ---- simple one‑item case ------------------------------------
        (
            [
                {"category": "cat1", "project": "projA", "study": "studyX"},
            ],
            {"cat1": {"projA": {"studyX"}}},
        ),
        # ---- two studies under the same project --------------------
        (
            [
                {"category": "cat1", "project": "projA", "study": "studyX"},
                {"category": "cat1", "project": "projA", "study": "studyY"},
            ],
            {"cat1": {"projA": {"studyX", "studyY"}}},
        ),
        # ---- duplicate studies should be deduplicated ---------------
        (
            [
                {"category": "cat1", "project": "projA", "study": "studyX"},
                {"category": "cat1", "project": "projA", "study": "studyX"},
            ],
            {"cat1": {"projA": {"studyX"}}},
        ),
        # ---- multiple categories / projects -------------------------
        (
            [
                {"category": "cat1", "project": "projA", "study": "s1"},
                {"category": "cat1", "project": "projB", "study": "s2"},
                {"category": "cat2", "project": "projC", "study": "s3"},
                {"category": "cat2", "project": "projC", "study": "s4"},
            ],
            {
                "cat1": {"projA": {"s1"}, "projB": {"s2"}},
                "cat2": {"projC": {"s3", "s4"}},
            },
        ),
    ],
)
def test_build_category_map(input_objs, expected_map):
    """
    Parameterised test that checks the nesting logic and the
    de‑duplication of studies.
    """
    # Act
    result = cli_mod._build_category_map(input_objs)

    # Assert – we want the exact same nested structure.
    # The result uses `defaultdict` under the hood, but for comparison we
    # convert it to plain dicts (and sets) so the equality test is clean.
    def to_plain(d):
        if isinstance(d, defaultdict):
            d = {k: to_plain(v) for k, v in d.items()}
        return d

    plain_result = to_plain(result)

    assert plain_result == expected_map


def test_build_category_map_is_robust_to_missing_keys():
    """
    The real code always receives dicts with the three keys,
    but it is useful to verify that a missing key raises a clear error.
    """
    malformed = [{"category": "c1", "project": "p1"}]  # no "study"

    with pytest.raises(KeyError) as excinfo:
        cli_mod._build_category_map(malformed)

    # The error message should contain the missing key name.
    assert "'study'" in str(excinfo.value)


# ----------------------------------------------------------------------
# Optional: integration‑style test that combines both helpers ------------
# ----------------------------------------------------------------------
def test_collect_and_build_integration(fake_cm, fake_profile):
    """
    End‑to‑end test that the two helpers together produce the expected
    nested mapping.  It uses a single mock for `query_mongo_obj`.
    """
    # Mock data returned per category
    mock_data = {
        "cat_A": [
            {"category": "cat_A", "project": "P1", "study": "S1"},
            {"category": "cat_A", "project": "P1", "study": "S2"},
        ],
        "cat_B": [
            {"category": "cat_B", "project": "P2", "study": "S3"},
        ],
    }

    def fake_query(filter_dict, profile):
        # Return the list that matches the incoming category filter.
        return mock_data.get(filter_dict["category"], [])

    with mock.patch.object(cli_mod, "query_mongo_obj", side_effect=fake_query):
        objs = cli_mod._collect_objects(fake_cm, fake_profile)

    # Now feed those objects into the mapper
    mapping = cli_mod._build_category_map(objs)

    expected = {
        "cat_A": {"P1": {"S1", "S2"}},
        "cat_B": {"P2": {"S3"}},
    }

    # Normalise `defaultdict` to plain dicts for assertion
    def normalize(d):
        if isinstance(d, defaultdict):
            d = {k: normalize(v) for k, v in d.items()}
        return d

    assert normalize(mapping) == expected
