def reify(relation: str):
    """
    Reifies a relation, lifting all relations that are connected to the original entities to the new entity.
    """
    return f"""
        MATCH (left:Entity)-[r:REL {{ RelationType: '{relation}' }}]->(right:Entity)
        CREATE (left)<-[:REL {{ RelationType: 'REIFIED', Source: '{relation}' }}]-(compound:Entity {{
                ID: left.ID + '+' + right.ID,
                EntityType: left.EntityType + '+' + right.EntityType,
                Compound: true
        }})-[:REL {{ RelationType: 'REIFIED', Source: '{relation}' }}]->(right)
        DELETE r
        
        WITH compound
        MATCH (compound)-[:REL {{ RelationType: 'REIFIED' }}]->(:Entity)-[r1:REL]->(target:Entity)
        WHERE r1.RelationType <> 'REIFIED'
        CREATE (compound)-[r1_copy:REL]->(target)
        SET r1_copy = properties(r1)
        DELETE r1
        UNION
        MATCH (compound)-[:REL {{ RelationType: 'REIFIED' }}]->(:Entity)<-[r2:REL]-(source:Entity)
        WHERE r2.RelationType <> 'REIFIED'
        CREATE (compound)<-[r2_copy:REL]-(source)
        SET r2_copy = properties(r2)
        DELETE r2
        """


def correlate_derived():
    """
    Correlates events to derived entities.
    """
    return f"""
        MATCH (e:Event)-[:CORR]->(:Entity)<-[:REL* {{ RelationType: 'REIFIED' }}]-(n:Entity {{ Compound:true }})
        CREATE (e)-[:CORR]->(n)
        """


def create_roots():
    """
    Creates root entities.
    """
    return f"""
        MATCH (n:Entity {{ Compound:true }}) 
        WHERE NOT (n)<-[:REL {{ RelationType: 'REIFIED' }}]-() 
              AND (n)-[:REL {{ RelationType: 'REIFIED' }}]->() 
        SET n:Entity:Root
    """


def create_df(events: tuple[str, ...] = None):
    """
    Finds the root entity and creates a DF relation between the correlated events.
    """
    return f"""
        MATCH (n:Entity:Root),
              (e:Event{" WHERE e.EventType IN ['" + "', '".join(events) + "']" if events else ""})-[:CORR]->(n)
        WITH n, e ORDER BY e.Timestamp, ID(e)
        WITH n, collect(e) AS events
        UNWIND range(0, size(events)-2) AS i
        WITH events[i] as first, events[i+1] as second
        MERGE (first)-[df:DF{f"_PROJECTION {{ ID: '{str(hash(events))}' }}" if events else ""}]->(second)    
    """


def verify_no_crossing_df_projections():
    """
    Verifies that there are no df-projections between events with different root nodes
    """
    return f"""
        MATCH (e1:Event)-[:DF_PROJECTION]-(e2:Event)
        WHERE (e1 <> e2) 
          AND NOT(EXISTS((e1)-[:CORR]->(:Entity:Root)<-[:CORR]-(e2)))
        RETURN *
    """


def verify_violating_events_have_df_projection():
    """
    Verifies that events that violate a control have a df-projection between them
    """
    return f"""
        MATCH (c:Control),
              (c)<-[:VIOLATES]-(e1:Event),
              (c)<-[:VIOLATES]-(e2:Event),
              (e1)-[:CORR]->(:Entity:Root)<-[:CORR]-(e2)
        WHERE (e1 <> e2) 
          AND NOT(EXISTS((e1)-[:DF_PROJECTION]-(e2)))
        RETURN *
    """
