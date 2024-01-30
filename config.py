from construct.controls.constraint import (
    Comparison,
    Attribute,
    ComparisonOperator,
    And as AndC,
    StrLiteral,
)
from construct.controls.control import Control
from construct.controls.expression import Implies, Event, Eventually, Constraint, And

neo4j_uri = "bolt://localhost:7687"
neo4j_auth = ("neo4j", "12341234")
neo4j_db_dir = r"E:\neo4j\relate-data\dbmss\dbms-1ea13b42-8416-4b56-ad78-f5db6911e301"
neo4j_import_dir = neo4j_db_dir + r"\import"
neo4j_bin_dir = neo4j_db_dir + r"\bin"

csv_import_dir = r"E:\thesis-data\CSVs"
activities_file = r"E:\thesis-data\changeActivityPurchase.xlsx"
transform_export_dir = neo4j_import_dir

company = "C110"

step_nuke_transformed = True

step_nuke_graph = True
step_construct_graph = True
step_reify_relations = True
step_create_roots = True
step_correlate_derived = True
step_create_df = True
step_check_controls = True
step_verify_construction = True
step_remove_projections = True
step_set_identities = True


sod_implication = lambda e1, e2, subset: Implies(
    Implies(Event("e1", e1), Eventually(Event("e2", e2))),
    Constraint(
        Comparison(
            Attribute("e1", "uID"),
            Attribute("e2", "uID"),
            ComparisonOperator.NOT_EQUALS,
        )
    ),
    subset,
)

subset_handelsartikelen = AndC(
    Comparison(
        Attribute("e1", "Item_Type"),
        StrLiteral("_Handelsartikelen"),
        ComparisonOperator.EQUALS,
    ),
    Comparison(
        Attribute("e2", "Item_Type"),
        StrLiteral("_Handelsartikelen"),
        ComparisonOperator.EQUALS,
    ),
)


controls = [
    Control(
        "Segregation of duties between Order/Receipt",
        sod_implication(
            "ORDER_LINE_CREATED", "RECEIPT_LINE_CREATED", subset_handelsartikelen
        ),
    ),
    Control(
        "Segregation of duties between Order/Invoice",
        sod_implication(
            "ORDER_LINE_CREATED", "INVOICE_LINE_CREATED", subset_handelsartikelen
        ),
    ),
    Control(
        "Segregation of duties between Receipt/Invoice",
        And(
            sod_implication(
                "RECEIPT_LINE_CREATED", "INVOICE_LINE_CREATED", subset_handelsartikelen
            ),
            sod_implication(
                "INVOICE_LINE_CREATED", "RECEIPT_LINE_CREATED", subset_handelsartikelen
            ),
        ),
    ),
    Control(
        "Segregation of duties between Invoice/Payment",
        And(
            sod_implication(
                "INVOICE_LINE_CREATED",
                "PAID",
                Comparison(
                    Attribute("e1", "Item_Type"),
                    StrLiteral("_Handelsartikelen"),
                    ComparisonOperator.EQUALS,
                ),
            ),
            sod_implication(
                "PAID",
                "INVOICE_LINE_CREATED",
                Comparison(
                    Attribute("e2", "Item_Type"),
                    StrLiteral("_Handelsartikelen"),
                    ComparisonOperator.EQUALS,
                ),
            ),
        ),
    ),
]
