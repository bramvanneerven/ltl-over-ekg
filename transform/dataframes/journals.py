import itertools

import polars as pl

from transform.loader import Loader

i = itertools.count()


def load(booked_as: pl.DataFrame):
    loader = Loader()

    unique_docs = booked_as.select("to_ID").unique()

    journals = (
        loader.get_df(
            "journals",
            columns=[
                "%GL_DOC",
                "%GL_ACC",
                "#GL Credit",
                "#GL Debit",
                "GL Entry Date",
                "GL Effective Date",
                "GL User",
            ],
        )
        .filter(pl.col("%GL_DOC").is_in(unique_docs["to_ID"]))
        .unique()
    )

    entry_creations = (
        journals.select(["%GL_DOC", "GL Effective Date", "GL User"])
        .unique(subset="%GL_DOC")
        .with_columns(
            [
                pl.lit("Journal entry created").alias("Activity"),
                (
                    pl.col("GL Effective Date").str.strptime(pl.Datetime)
                    + pl.duration(hours=21 + next(i))
                ).alias("Timestamp"),
            ]
        )
        .drop("GL Effective Date")
        .rename({"%GL_DOC": "GL_Doc", "GL User": "uID"})
    )

    entities = entry_creations.select("GL_Doc").unique().rename({"GL_Doc": "ID"})

    creation_of_relation = (
        entities.with_columns(pl.col("ID").alias("to_ID"))
        .rename({"ID": "from_GL_Doc"})
        .unique()
    )

    modified_relations = (
        journals.select(["%GL_DOC", "%GL_ACC", "#GL Credit", "#GL Debit"])
        .group_by(["%GL_DOC", "%GL_ACC"])
        .agg(pl.sum("#GL Credit"), pl.sum("#GL Debit"))
        .rename(
            {
                "%GL_DOC": "from_GL_Doc",
                "%GL_ACC": "to_ID",
                "#GL Credit": "GL_Credit",
                "#GL Debit": "GL_Debit",
            }
        )
    )

    return entry_creations, modified_relations, entities, creation_of_relation
