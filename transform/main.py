import sys

import config as cfg
from dataframes import (
    order_change_events,
    order_creation_events,
    order_entities,
    order_relations,
    receipts,
    journals,
    invoice_events,
    invoice_entities,
    invoice_relations,
    payments,
    account_entities,
    users,
)
from graph_construct import Event, Entity, Relation
from saver import Saver
from util import execute

s = Saver(cfg.step_nuke_transformed)

order_e = {
    "header_change": Event("order_header_changed"),
    "line_change": Event("order_line_changed"),
    "line_creation": Event("order_line_created"),
}
order_o = {"header": Entity("order_header"), "line": Entity("order_line")}
order_r = {
    "change_of_header": Relation(
        "change_of", order_e["header_change"], order_o["header"]
    ),
    "change_of_line": Relation("change_of", order_e["line_change"], order_o["line"]),
    "creation_of_line": Relation(
        "creation_of", order_e["line_creation"], order_o["line"]
    ),
    "part_of_header": Relation("part_of", order_o["line"], order_o["header"]),
}


journal_e = {
    "payment_creation": Event("payment_journal_entry_created"),
    "invoice_creation": Event("invoice_journal_entry_created"),
    "receipt_creation": Event("receipt_journal_entry_created"),
}
journal_o = {
    "payment_entry": Entity("payment_journal_entry"),
    "invoice_entry": Entity("invoice_journal_entry"),
    "receipt_entry": Entity("receipt_journal_entry"),
    "account": Entity("account"),
}
journal_r = {
    "payment_modifies": Relation(
        "modifies", journal_e["payment_creation"], journal_o["account"]
    ),
    "payment_creation_of": Relation(
        "creation_of", journal_e["payment_creation"], journal_o["payment_entry"]
    ),
    "invoice_modifies": Relation(
        "modifies", journal_e["invoice_creation"], journal_o["account"]
    ),
    "invoice_creation_of": Relation(
        "creation_of", journal_e["invoice_creation"], journal_o["invoice_entry"]
    ),
    "receipt_modifies": Relation(
        "modifies", journal_e["receipt_creation"], journal_o["account"]
    ),
    "receipt_creation_of": Relation(
        "creation_of", journal_e["receipt_creation"], journal_o["receipt_entry"]
    ),
}


receipt_e = {"creation": Event("receipt_line_created")}
receipt_o = {"header": Entity("receipt_header"), "line": Entity("receipt_line")}
receipt_r = {
    "creation_of": Relation("creation_of", receipt_e["creation"], receipt_o["line"]),
    "booked_as": Relation("booked_as", receipt_o["header"], journal_o["receipt_entry"]),
    "for": Relation("receipt_for", receipt_o["line"], order_o["line"]),
    "part_of": Relation("part_of", receipt_o["line"], receipt_o["header"]),
}


invoice_e = {"creation": Event("invoice_line_created")}
invoice_o = {"header": Entity("invoice_header"), "line": Entity("invoice_line")}
invoice_r = {
    "creation_of": Relation("creation_of", invoice_e["creation"], invoice_o["line"]),
    "booked_as": Relation("booked_as", invoice_o["header"], journal_o["invoice_entry"]),
    "for": Relation("invoice_for", invoice_o["line"], order_o["line"]),
    "part_of": Relation("part_of", invoice_o["line"], invoice_o["header"]),
}


payment_e = {"paid": Event("paid")}
payment_o = {"payment": Entity("payment")}
payment_r = {
    "creation_of": Relation("creation_of", payment_e["paid"], payment_o["payment"]),
    "booked_as": Relation(
        "booked_as", payment_o["payment"], journal_o["payment_entry"]
    ),
    "for": Relation("payment_for", payment_o["payment"], invoice_o["header"]),
}


user_o = {"user": Entity("user")}


def user_performed(event: Event):
    return Relation("performed", user_o["user"], event)


def transform():
    # --- ORDERS ---
    # -- events --
    df_order_header_changes, df_order_line_changes = order_change_events.load()
    s.save_df(df_order_header_changes, order_e["header_change"])
    s.save_df(df_order_line_changes, order_e["line_change"])

    df_order_line_creations = order_creation_events.load()
    s.save_df(df_order_line_creations, order_e["line_creation"])

    # -- entities (objects) --
    df_order_header_entities, df_order_line_entities = order_entities.load(
        df_order_line_creations
    )
    s.save_df(df_order_header_entities, order_o["header"])
    s.save_df(df_order_line_entities, order_o["line"])

    # -- relations --
    (
        df_order_header_relation_change,
        df_order_line_relation_change,
        df_order_line_relation_creation,
        df_order_line_relation_part_of_header,
    ) = order_relations.load(
        df_order_header_changes, df_order_line_changes, df_order_line_creations
    )
    s.save_df(df_order_header_relation_change, order_r["change_of_header"])
    s.save_df(df_order_line_relation_change, order_r["change_of_line"])
    s.save_df(df_order_line_relation_creation, order_r["creation_of_line"])
    s.save_df(df_order_line_relation_part_of_header, order_r["part_of_header"])

    df_order_header_relation_performed_change = users.load_relations(
        df_order_header_changes
    )
    df_order_line_relation_performed_change = users.load_relations(
        df_order_line_changes
    )
    df_order_line_relation_performed_creation = users.load_relations(
        df_order_line_creations
    )
    s.save_df(
        df_order_header_relation_performed_change,
        user_performed(order_e["header_change"]),
    )
    s.save_df(
        df_order_line_relation_performed_change, user_performed(order_e["line_change"])
    )
    s.save_df(
        df_order_line_relation_performed_creation,
        user_performed(order_e["line_creation"]),
    )

    # --- RECEIPTS ---
    (
        df_receipt_events,
        df_receipt_header_entities,
        df_receipt_line_entities,
        df_receipt_creation,
        df_receipt_relation_booked,
        df_receipt_relation_for,
        df_receipt_relation_part_of,
    ) = receipts.load()

    # -- events --
    s.save_df(df_receipt_events, receipt_e["creation"])

    # -- entities (objects) --
    s.save_df(df_receipt_header_entities, receipt_o["header"])
    s.save_df(df_receipt_line_entities, receipt_o["line"])

    # -- relations --
    s.save_df(df_receipt_creation, receipt_r["creation_of"])
    s.save_df(df_receipt_relation_booked, receipt_r["booked_as"])
    s.save_df(df_receipt_relation_for, receipt_r["for"])
    s.save_df(df_receipt_relation_part_of, receipt_r["part_of"])

    df_receipt_relation_performed = users.load_relations(df_receipt_events)
    s.save_df(df_receipt_relation_performed, user_performed(receipt_e["creation"]))

    # --- INVOICES ---
    # -- events --
    df_invoice_events = invoice_events.load()
    s.save_df(df_invoice_events, invoice_e["creation"])

    # -- entities (objects) --
    df_invoice_lines, df_invoice_headers = invoice_entities.load(df_invoice_events)
    s.save_df(df_invoice_lines, invoice_o["line"])
    s.save_df(df_invoice_headers, invoice_o["header"])

    # -- relations --
    (
        df_invoice_relation_creation,
        df_invoice_relation_booked,
        df_invoice_relation_for,
        df_invoice_relation_part_of,
    ) = invoice_relations.load(df_invoice_events)
    s.save_df(df_invoice_relation_creation, invoice_r["creation_of"])
    s.save_df(df_invoice_relation_booked, invoice_r["booked_as"])
    s.save_df(df_invoice_relation_for, invoice_r["for"])
    s.save_df(df_invoice_relation_part_of, invoice_r["part_of"])

    df_invoice_relation_performed = users.load_relations(df_invoice_events)
    s.save_df(df_invoice_relation_performed, user_performed(invoice_e["creation"]))

    # --- PAYMENTS (clearances) ---
    (
        df_payment_events,
        df_payment_entities,
        df_payment_relation_creation,
        df_payment_relation_booked,
        df_payment_relation_for,
    ) = payments.load()

    # -- events --
    s.save_df(df_payment_events, payment_e["paid"])

    # -- entities (objects) --
    s.save_df(df_payment_entities, payment_o["payment"])

    # -- relations --
    s.save_df(df_payment_relation_creation, payment_r["creation_of"])
    s.save_df(df_payment_relation_booked, payment_r["booked_as"])
    s.save_df(df_payment_relation_for, payment_r["for"])

    df_payment_relation_performed = users.load_relations(df_payment_events)
    s.save_df(df_payment_relation_performed, user_performed(payment_e["paid"]))

    # --- JOURNAL ENTRIES ---
    (
        df_receipt_journal_entry_event_creations,
        df_receipt_journal_entry_relation_modified,
        df_receipt_journal_entry_entities,
        df_receipt_journal_entry_relation_creation,
    ) = journals.load(df_receipt_relation_booked)
    s.save_df(df_receipt_journal_entry_event_creations, journal_e["receipt_creation"])
    s.save_df(df_receipt_journal_entry_relation_modified, journal_r["receipt_modifies"])
    s.save_df(df_receipt_journal_entry_entities, journal_o["receipt_entry"])
    s.save_df(
        df_receipt_journal_entry_relation_creation, journal_r["receipt_creation_of"]
    )

    df_receipt_journal_entry_relations_performed = users.load_relations(
        df_receipt_journal_entry_event_creations
    )
    s.save_df(
        df_receipt_journal_entry_relations_performed,
        user_performed(journal_e["receipt_creation"]),
    )

    (
        df_invoice_journal_entry_event_creations,
        df_invoice_journal_entry_relation_modified,
        df_invoice_journal_entry_entities,
        df_invoice_journal_entry_relation_creation,
    ) = journals.load(df_invoice_relation_booked)
    s.save_df(df_invoice_journal_entry_event_creations, journal_e["invoice_creation"])
    s.save_df(df_invoice_journal_entry_relation_modified, journal_r["invoice_modifies"])
    s.save_df(df_invoice_journal_entry_entities, journal_o["invoice_entry"])
    s.save_df(
        df_invoice_journal_entry_relation_creation, journal_r["invoice_creation_of"]
    )

    df_invoice_journal_entry_relations_performed = users.load_relations(
        df_invoice_journal_entry_event_creations
    )
    s.save_df(
        df_invoice_journal_entry_relations_performed,
        user_performed(journal_e["invoice_creation"]),
    )

    (
        df_payment_journal_entry_event_creations,
        df_payment_journal_entry_relation_modified,
        df_payment_journal_entry_entities,
        df_payment_journal_entry_relation_creation,
    ) = journals.load(df_payment_relation_booked)
    s.save_df(df_payment_journal_entry_event_creations, journal_e["payment_creation"])
    s.save_df(df_payment_journal_entry_relation_modified, journal_r["payment_modifies"])
    s.save_df(df_payment_journal_entry_entities, journal_o["payment_entry"])
    s.save_df(
        df_payment_journal_entry_relation_creation, journal_r["payment_creation_of"]
    )

    df_payment_journal_entry_relations_performed = users.load_relations(
        df_payment_journal_entry_event_creations
    )

    # --- ACCOUNTS ---
    df_account_entities = account_entities.load(
        [
            df_receipt_journal_entry_relation_modified,
            df_invoice_journal_entry_relation_modified,
            df_payment_journal_entry_relation_modified,
        ]
    )
    s.save_df(df_account_entities, journal_o["account"])

    # --- USERS ---
    df_user_entities = users.load_entities(
        [
            df_order_header_changes,
            df_order_line_changes,
            df_order_line_creations,
            df_receipt_events,
            df_receipt_journal_entry_event_creations,
            df_invoice_journal_entry_event_creations,
            df_payment_journal_entry_event_creations,
            df_invoice_events,
            df_payment_events,
        ]
    )
    s.save_df(df_user_entities, user_o["user"])


execute("Transform", transform)
sys.exit()
