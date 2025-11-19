"""Graph schema definition and initialization."""

from falkor.graph.client import Neo4jClient


class GraphSchema:
    """Manages graph schema creation and constraints."""

    # Constraint definitions
    CONSTRAINTS = [
        # Uniqueness constraints
        "CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.filePath IS UNIQUE",
        "CREATE CONSTRAINT class_qualified_name_unique IF NOT EXISTS FOR (c:Class) REQUIRE c.qualifiedName IS UNIQUE",
        "CREATE CONSTRAINT function_qualified_name_unique IF NOT EXISTS FOR (f:Function) REQUIRE f.qualifiedName IS UNIQUE",
    ]

    # Index definitions for performance
    INDEXES = [
        "CREATE INDEX file_path_idx IF NOT EXISTS FOR (f:File) ON (f.filePath)",
        "CREATE INDEX file_language_idx IF NOT EXISTS FOR (f:File) ON (f.language)",
        "CREATE INDEX class_name_idx IF NOT EXISTS FOR (c:Class) ON (c.qualifiedName)",
        "CREATE INDEX function_name_idx IF NOT EXISTS FOR (f:Function) ON (f.qualifiedName)",
        "CREATE INDEX concept_name_idx IF NOT EXISTS FOR (c:Concept) ON (c.name)",
        # Full-text search indexes
        "CREATE FULLTEXT INDEX function_docstring_idx IF NOT EXISTS FOR (f:Function) ON EACH [f.docstring]",
        "CREATE FULLTEXT INDEX class_docstring_idx IF NOT EXISTS FOR (c:Class) ON EACH [c.docstring]",
    ]

    def __init__(self, client: Neo4jClient):
        """Initialize schema manager.

        Args:
            client: Neo4j client instance
        """
        self.client = client

    def create_constraints(self) -> None:
        """Create all uniqueness constraints."""
        for constraint in self.CONSTRAINTS:
            try:
                self.client.execute_query(constraint)
            except Exception as e:
                print(f"Warning: Could not create constraint: {e}")

    def create_indexes(self) -> None:
        """Create all indexes."""
        for index in self.INDEXES:
            try:
                self.client.execute_query(index)
            except Exception as e:
                print(f"Warning: Could not create index: {e}")

    def initialize(self) -> None:
        """Initialize complete schema."""
        print("Creating graph schema...")
        self.create_constraints()
        self.create_indexes()
        print("Schema created successfully!")

    def drop_all(self) -> None:
        """Drop all constraints and indexes. Use with caution!"""
        import re

        # Validate name is safe (alphanumeric, underscore, hyphen only)
        def is_safe_name(name: str) -> bool:
            return bool(re.match(r'^[a-zA-Z0-9_-]+$', name))

        # Drop all constraints
        drop_constraints_query = """
        SHOW CONSTRAINTS
        YIELD name
        RETURN name
        """
        constraints = self.client.execute_query(drop_constraints_query)
        for record in constraints:
            name = record["name"]
            if is_safe_name(name):
                # Safe to use f-string since we validated the name
                self.client.execute_query(f"DROP CONSTRAINT {name}")
            else:
                print(f"Warning: Skipping constraint with unsafe name: {name}")

        # Drop all indexes
        drop_indexes_query = """
        SHOW INDEXES
        YIELD name
        WHERE name <> 'node_label_index' AND name <> 'relationship_type_index'
        RETURN name
        """
        indexes = self.client.execute_query(drop_indexes_query)
        for record in indexes:
            name = record["name"]
            if is_safe_name(name):
                # Safe to use f-string since we validated the name
                self.client.execute_query(f"DROP INDEX {name}")
            else:
                print(f"Warning: Skipping index with unsafe name: {name}")

        print("Schema dropped!")
