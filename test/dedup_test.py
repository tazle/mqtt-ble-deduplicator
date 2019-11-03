from mqtt_ble_deduplicator import dedup

def test__check__empty_deduplicator__returns_false_for_not_duplicate():
    deduper = dedup.Deduplicator()
    assert deduper.check("foo") == False

def test__check__item_in_deduplicator__returns_true_for_duplicate():
    deduper = dedup.Deduplicator()
    deduper.add("foo")
    assert deduper.check("foo") == True

def test__add__when_full__item_purged_from_deduplicator():
    deduper = dedup.Deduplicator(max_size=1)
    deduper.add("foo")
    deduper.add("bar")
    assert deduper.check("foo") == False

def test__add__on_purge__removes_first_item():
    deduper = dedup.Deduplicator(max_size=2)
    deduper.add("foo")
    deduper.add("bar")
    deduper.add("xyzzy")
    assert deduper.check("xyzzy") == True
    assert deduper.check("bar") == True
    assert deduper.check("foo") == False
