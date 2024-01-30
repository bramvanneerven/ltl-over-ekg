import locale

import plotly.graph_objects as go
import polars as pl
import streamlit as st
from streamlit_agraph import agraph, Node, Config, Edge

import config as cfg
from driver import Driver

locale.setlocale(locale.LC_ALL, "")

cs = [f"c{x}" for x in range(len(cfg.controls))]


@st.cache_data
def load_initial():
    driver = Driver()

    invoice_amounts = driver.query(
        f"""
            MATCH (e:Event {{ EventType: 'INVOICE_LINE_CREATED' }})
            WHERE e.PI_Line_No CONTAINS "-2022" AND e.Item_Type = "_Handelsartikelen"
            WITH e.PI_No AS invoice, e.PI_Line_No AS invoice_line, round(toFloat(e.Quantity) * toFloat(e.Price) * toFloat(e.Exchange_Rate), 2) AS amount
            RETURN *
        """
    )

    invoice_violations = driver.query(
        f"""
            MATCH (e:Event {{ EventType: "INVOICE_LINE_CREATED" }}), 
                  (e)-[:CORR]->(:Entity:Root)<-[:CORR]-(:Event)-[:VIOLATES]->(c:Control)
            WHERE e.PI_Line_No CONTAINS "-2022" AND e.Item_Type = "_Handelsartikelen"
            WITH e.PI_Line_No AS invoice_line, collect(DISTINCT c.ID) AS violations
            WITH invoice_line, {','.join([f'"{c}" IN violations as {c}' for c in cs])}
            RETURN *
        """
    )

    df_amounts = pl.DataFrame(invoice_amounts)
    df_violations = pl.DataFrame(invoice_violations)
    df_combined = (
        df_amounts.join(df_violations, on="invoice_line", how="left")
        .with_columns(pl.all().fill_null(pl.lit(False)))
        .with_columns(pl.any_horizontal(*cs).alias("has_violation"))
    )

    return df_combined


@st.cache_data
def load_total():
    df_combined = load_initial()
    return df_combined.select("amount").sum().row(0)[0]


def load_total_category(
    control: str, category: str, amount_threshold: float, use_absolute: bool = False
):
    df_combined = load_data(amount_threshold, use_absolute)

    return (
        df_combined.select(control, "amount")
        .filter(pl.col(control) == category)
        .select("amount")
        .sum()
        .row(0)[0]
    )


@st.cache_data
def load_data(amount_threshold: float, use_absolute: bool = False):
    df_combined = load_initial()

    for c in cs:
        df_combined = df_combined.with_columns(
            pl.when(pl.col(c).cast(pl.Boolean).not_() | pl.col(c).is_null())
            .then(pl.lit("Non-violation"))
            .otherwise(
                pl.when(
                    (
                        pl.col("amount").cast(pl.Float64).abs()
                        if use_absolute
                        else pl.col("amount").cast(pl.Float64)
                    )
                    <= amount_threshold
                )
                .then(pl.lit("Violation below threshold"))
                .otherwise(pl.lit("Violation"))
            )
            .alias(c)
        )

    return df_combined


@st.cache_data
def load_summary_data(
    control: str, category: str, amount_threshold: float, use_absolute: bool = False
):
    df_initial = load_data(amount_threshold, use_absolute)
    df_agg = (
        df_initial.select(control, "amount")
        .filter(pl.col(control) == category)
        .group_by(control)
        .agg(pl.col("amount").sum())
    )

    if df_agg.shape[0] == 0:
        return 0
    else:
        return df_agg.item(0, 1)


@st.cache_data
def load_detailed_data(
    amount_threshold: float, only_show_violations: bool, use_absolute: bool = False
):
    df_initial = load_data(amount_threshold, use_absolute)
    df_selected = df_initial.select("invoice_line", "amount", "has_violation", *cs)

    if only_show_violations:
        df_selected = df_selected.filter(pl.col("has_violation"))

    return df_selected.sort("amount", descending=True)


@st.cache_data
def load_process_instance_data(line_no: str):
    driver = Driver()

    data = driver.query(
        f"""
        MATCH (:Event {{ EventType: "INVOICE_LINE_CREATED", PI_Line_No: "{line_no}" }})-[:CORR]->(r:Root)
        WITH r
        MATCH (r)<-[:CORR]-(e:Event),
              (e)-[c:CORR]->(n:Entity WHERE n.Compound IS NULL),
              (r)<-[:CORR]-(:Event)-[df:DF]->(:Event)-[:CORR]->(r)
        OPTIONAL MATCH (r)<-[:CORR]-(:Event)-[v:VIOLATES]-(k:Control)
        WITH collect(DISTINCT e) AS events, 
             collect(DISTINCT c) AS corr,
             collect(DISTINCT n) AS entities, 
             collect(DISTINCT df) AS df, 
             collect(DISTINCT v) AS violations, 
             collect(DISTINCT k) AS controls
        UNWIND corr AS c
        WITH events, collect(c) AS corr, entities, df, violations, controls, collect(properties(c)) AS corr_properties
        RETURN *
    """
    )

    e = data[0]["events"]
    c = zip(data[0]["corr"], data[0]["corr_properties"])
    n = data[0]["entities"]
    d = data[0]["df"]
    v = data[0]["violations"]
    k = data[0]["controls"]

    return e, c, n, d, v, k


colors = {
    "Non-violation": "green",
    "Violation below threshold": "orange",
    "Violation": "red",
}

st.set_page_config(layout="wide", page_title="Control Violations")

st.markdown(
    """
        <style>
            .appview-container .main .block-container {{
                padding-top: {padding_top}rem;
                padding-bottom: {padding_bottom}rem;
                }}

        </style>""".format(
        padding_top=2, padding_bottom=1
    ),
    unsafe_allow_html=True,
)

with st.sidebar:
    st.title("Settings")
    threshold = st.number_input("Threshold invoice value", value=20000, step=1000)
    absolute = st.toggle(
        "Use absolute value",
        value=True,
        help="Will use the absolute value of amounts when comparing to the threshold",
    )
    st.write(
        f"""Invoice lines with {'an absolute ' if absolute else 'a'} value less than or equal to
        {locale.currency(threshold, grouping=True)} will be considered violations, but are 
        :{colors["Violation below threshold"]}[marked] differently."""
    )

    with st.container():
        instance_select_placeholder = st.empty()
        only_violations = st.toggle(
            "Only show violations",
            value=True,
            help="Only show violations as options for the detailed view",
        )

summary_fig = go.Figure(
    layout=go.Layout(
        xaxis=dict(range=[0, load_total()]),
        barmode="stack",
        showlegend=False,
        height=225,
        margin={"t": 0, "l": 0, "b": 0, "r": 0},
    )
)

summary_fig.add_trace(
    go.Bar(
        x=[load_summary_data(c, "Non-violation", threshold, absolute) for c in cs],
        y=cs,
        name="Non-violation",
        orientation="h",
        marker=dict(color=colors["Non-violation"]),
        text=[
            f"€ {round(load_total_category(c,'Non-violation', threshold, absolute) / 1_000_000, 2)}M"
            for c in cs
        ],
    )
)

summary_fig.add_trace(
    go.Bar(
        x=[
            load_summary_data(c, "Violation below threshold", threshold, absolute)
            for c in cs
        ],
        y=cs,
        name="Violation below threshold",
        orientation="h",
        marker=dict(color=colors["Violation below threshold"]),
        text=[
            f"€ {round(load_total_category(c,'Violation below threshold', threshold, absolute) / 1_000_000, 2)}M"
            for c in cs
        ],
    )
)

summary_fig.add_trace(
    go.Bar(
        x=[load_summary_data(c, "Violation", threshold, absolute) for c in cs],
        y=cs,
        name="Violation",
        orientation="h",
        marker=dict(color=colors["Violation"]),
        text=[
            f"€ {round(load_total_category(c,'Violation', threshold, absolute) / 1_000_000, 2)}M"
            for c in cs
        ],
    )
)

detailed_data = load_detailed_data(threshold, only_violations, absolute)

selected_process_instance = instance_select_placeholder.selectbox(
    "Process instance", detailed_data["invoice_line"].to_list(), key="process_instance"
)

chart_tab, table_tab = st.tabs(["Chart", "Table"])

with chart_tab:
    st.write("### Value per violation type")
    st.plotly_chart(summary_fig, use_container_width=True)

with table_tab:
    st.dataframe(
        detailed_data,
        hide_index=True,
        use_container_width=True,
        column_config=dict(
            amount=st.column_config.NumberColumn("Amount", format="€ %.2f")
        ),
    )

st.write("### Process instance")
st.caption(
    f'{selected_process_instance} ({locale.currency(detailed_data.select("amount", "invoice_line").filter(pl.col("invoice_line") == selected_process_instance).item(0, 0), grouping=True)})'
)
with st.container(border=True):
    (events, corr, entities, df, violations, controls) = load_process_instance_data(
        selected_process_instance
    )

    violations_2D = [[v[0]["ident"], v[2]["ident"]] for v in violations]
    violations_1D = [item for sub_list in violations_2D for item in sub_list]
    event_nodes = [
        Node(
            id=e["ident"],
            label=e["EventType"],
            title=f'{e["Activity"]} [{e["Timestamp"].strftime("%Y/%m/%d, %H:%M:%S")}]',
            shape="square",
            color="red" if e["ident"] in violations_1D else "#00A19A",
        )
        for e in events
    ]
    corr_edges = [
        Edge(
            source=c[0][0]["ident"],
            target=c[0][2]["ident"],
            label=c[1]["RelationType"],
            title=(
                (float(c[1]["GL_Debit"]) - float(c[1]["GL_Credit"]))
                if c[1]["RelationType"] == "MODIFIES"
                else None
            ),
        )
        for c in corr
    ]
    df_edges = [
        Edge(
            source=d[0]["ident"],
            target=d[2]["ident"],
            label="DF",
            width=5,
            color="#00A19A",
        )
        for d in df
    ]
    entities_nodes = [
        Node(
            id=n["ident"],
            label=n["EntityType"],
            title=(
                f'{n["GL_Acc_Desc"]} ({n["GL_Acc_Type"]})'
                if n["EntityType"] == "ACCOUNT"
                else n["ID"]
            ),
            shape="dot",
            color="#A9A9A9",
        )
        for n in entities
    ]
    violation_edges = [
        Edge(
            source=v[0]["ident"],
            target=v[2]["ident"],
            label="VIOLATES",
            color="orange",
            width=2,
        )
        for v in violations
    ]
    control_nodes = [
        Node(
            id=k["ident"],
            label=k["ID"],
            title=k["Description"],
            shape="diamond",
            color="orange",
        )
        for k in controls
    ]

    agraph(
        [*event_nodes, *entities_nodes, *control_nodes],
        [*corr_edges, *df_edges, *violation_edges],
        Config(
            directed=True,
            height=600,
            width="100%",
            link={"labelProperty": "label", "renderLabel": True},
            physics=False,
            layout=dict(
                improvedLayout=True,
                hierarchical=dict(
                    enabled=True,
                    levelSeparation=150,
                    nodeSpacing=100,
                    treeSpacing=200,
                    blockShifting=True,
                    edgeMinimization=True,
                    parentCentralization=True,
                    direction="LR",
                    sortMethod="directed",
                    shakeTowards="roots",
                ),
            ),
        ),
    )
