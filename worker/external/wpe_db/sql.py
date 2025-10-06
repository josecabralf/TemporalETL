from psycopg2 import sql


class SQLQuery:
    @staticmethod
    def insert_events(table_name: str):
        """Generate the SQL query for inserting events into the specified table."""
        return sql.SQL("""
            INSERT INTO {} (
                source_kind_id, 
                parent_item_id, 
                event_id, 
                event_type, 
                relation_type, 
                employee_id, 
                event_time_utc, 
                week, 
                timezone, 
                event_time, 

                event_properties, 
                relation_properties, 
                metrics,
                version,
                specific_version
            ) 
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id
        """).format(sql.Identifier(table_name))

    @staticmethod
    def create_events_table(table_name: str):
        """Generate the SQL query for creating the specified table."""
        return sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                source_kind_id VARCHAR NOT NULL,
                parent_item_id VARCHAR,
                event_id VARCHAR NOT NULL UNIQUE,

                event_type VARCHAR NOT NULL,
                relation_type VARCHAR NOT NULL,

                employee_id VARCHAR NOT NULL,

                event_time_utc TIMESTAMP NOT NULL,
                week DATE NOT NULL,
                timezone VARCHAR,
                event_time TIMESTAMP,

                event_properties JSONB,
                relation_properties JSONB,
                metrics JSONB,

                version VARCHAR,
                specific_version VARCHAR
            );
        """).format(sql.Identifier(table_name))

    @staticmethod
    def update_event_properties(table_name: str):
        """Generate the SQL query for updating event properties."""
        return sql.SQL("""
            UPDATE {} 
            SET 
                event_properties = data.event_properties::jsonb, 
                metrics = data.metrics::jsonb,
                version = data.version,
                specific_version = data.specific_version
            FROM (VALUES %s) AS data(
                event_id,
                event_properties, 
                metrics,
                version,
                specific_version
            )
            WHERE {}.event_id = data.event_id
        """).format(sql.Identifier(table_name), sql.Identifier(table_name))
