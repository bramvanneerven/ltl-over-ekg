import config as cfg
from construct import query
from driver import Driver, SystemDriver
from load.loader import EventLoader, EntityLoader, RelationIndexLoader, RelationLoader
from util import execute


def nuke_graph():
    sys_driver = SystemDriver()
    sys_driver.drop_database()
    sys_driver.create_database()


if cfg.step_nuke_graph:
    execute("Nuke graph", nuke_graph)


def construct_graph():
    driver = Driver()
    driver.query("CREATE INDEX IF NOT EXISTS FOR (e:Event) ON (e.EventType)")
    driver.query("CREATE INDEX IF NOT EXISTS FOR (e:Event) ON (e.Timestamp)")
    driver.query("CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.EntityType)")
    driver.query("CREATE INDEX IF NOT EXISTS FOR ()-[r:REL]-() ON (r.RelationType)")
    driver.query("CREATE INDEX IF NOT EXISTS FOR ()-[c:CORR]-() ON (c.RelationType)")

    for i in RelationIndexLoader():
        for q in i.query():
            driver.query(q)

    for e in EventLoader():
        driver.query(e.query())

    for e in EntityLoader():
        driver.query(e.query())

    for r in RelationLoader():
        driver.query(r.query())


if cfg.step_construct_graph:
    execute("Load data", construct_graph)


def reify_relations():
    driver = Driver()
    rels = ["PART_OF", "BOOKED_AS", "INVOICE_FOR", "PAYMENT_FOR", "RECEIPT_FOR"]

    for rel in rels:
        driver.query(query.reify(rel))


if cfg.step_reify_relations:
    execute("Reify relations", reify_relations)


def correlate_derived():
    driver = Driver()
    driver.query(query.correlate_derived())


if cfg.step_correlate_derived:
    execute("Correlate derived entities to events", correlate_derived)


def create_roots():
    driver = Driver()
    driver.query(query.create_roots())


if cfg.step_create_roots:
    execute("Mark root entities", create_roots)


def create_df():
    driver = Driver()
    driver.query(query.create_df())

    dfs = {}
    for control in cfg.controls:
        events = control.find_events()
        dfs[hash(events)] = events

    for events in dfs.values():
        driver.query(query.create_df(events))


if cfg.step_create_df:
    execute("Create directly-follows paths", create_df)


def check_controls():
    driver = Driver()
    for control in cfg.controls:
        driver.query(control.check_query())


if cfg.step_check_controls:
    execute("Evaluate controls", check_controls)


def verify_construction():
    driver = Driver()
    assert driver.query(query.verify_no_crossing_df_projections()) == []
    assert driver.query(query.verify_violating_events_have_df_projection()) == []


if cfg.step_verify_construction:
    execute("Verify construction", verify_construction)


def remove_projections():
    driver = Driver()
    driver.query("MATCH ()-[p:DF_PROJECTION]-() DELETE p")


if cfg.step_remove_projections:
    execute("Remove projections", remove_projections)


def set_identities():
    driver = Driver()
    driver.query("MATCH (n) SET n.ident = elementID(n)")


if cfg.step_set_identities:
    execute("Set identities", set_identities)
