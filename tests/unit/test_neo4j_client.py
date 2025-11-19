"""Unit tests for Neo4jClient."""

from unittest.mock import Mock, MagicMock, patch

import pytest

from falkor.graph.client import Neo4jClient
from falkor.models import FileEntity, ClassEntity, FunctionEntity, Relationship, RelationshipType, NodeType


@pytest.fixture
def mock_driver():
    """Create a mock Neo4j driver."""
    driver = MagicMock()
    session = MagicMock()
    result = MagicMock()

    # Setup mock chain: driver -> session (as context manager) -> result
    driver.session.return_value.__enter__.return_value = session
    driver.session.return_value.__exit__.return_value = None
    session.run.return_value = result
    result.__iter__.return_value = []

    return driver


@pytest.fixture
def client(mock_driver):
    """Create a Neo4jClient with mocked driver."""
    with patch('falkor.graph.client.GraphDatabase') as mock_gd:
        mock_gd.driver.return_value = mock_driver
        client = Neo4jClient(
            uri="bolt://localhost:7687",
            username="neo4j",
            password="test"
        )
        yield client
        client.close()


class TestConnection:
    """Test database connection management."""

    def test_client_initialization(self, mock_driver):
        """Test client initializes with correct parameters."""
        with patch('falkor.graph.client.GraphDatabase') as mock_gd:
            mock_gd.driver.return_value = mock_driver

            client = Neo4jClient(
                uri="bolt://test:7687",
                username="testuser",
                password="testpass"
            )

            mock_gd.driver.assert_called_once()
            call_args = mock_gd.driver.call_args
            assert call_args[0][0] == "bolt://test:7687"

            client.close()

    def test_context_manager(self, mock_driver):
        """Test client works as context manager."""
        with patch('falkor.graph.client.GraphDatabase') as mock_gd:
            mock_gd.driver.return_value = mock_driver

            with Neo4jClient() as client:
                assert client.driver is not None

            mock_driver.close.assert_called_once()


class TestQueryExecution:
    """Test query execution methods."""

    def test_execute_query_simple(self, client, mock_driver):
        """Test executing a simple query."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [{"count": 5}]
        mock_session.run.return_value = mock_result

        result = client.execute_query("MATCH (n) RETURN count(n) as count")

        assert len(result) == 1
        assert result[0]["count"] == 5
        mock_session.run.assert_called_once()

    def test_execute_query_with_parameters(self, client, mock_driver):
        """Test executing query with parameters."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_session.run.return_value = mock_result

        params = {"name": "test"}
        client.execute_query("MATCH (n {name: $name}) RETURN n", params)

        call_args = mock_session.run.call_args
        assert call_args[0][1] == params


class TestNodeOperations:
    """Test node creation and management."""

    def test_batch_create_nodes_single_type(self, client, mock_driver):
        """Test batch creating nodes of single type."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [
            {"id": "elem1", "qualifiedName": "test.py"},
            {"id": "elem2", "qualifiedName": "test2.py"}
        ]
        mock_session.run.return_value = mock_result

        entities = [
            FileEntity(
                name="test.py",
                qualified_name="test.py",
                file_path="test.py",
                line_start=1,
                line_end=10,
                language="python",
                loc=10
            ),
            FileEntity(
                name="test2.py",
                qualified_name="test2.py",
                file_path="test2.py",
                line_start=1,
                line_end=20,
                language="python",
                loc=20
            )
        ]

        id_mapping = client.batch_create_nodes(entities)

        assert len(id_mapping) == 2
        assert "test.py" in id_mapping
        assert "test2.py" in id_mapping
        mock_session.run.assert_called()

    def test_batch_create_nodes_multiple_types(self, client, mock_driver):
        """Test batch creating nodes of multiple types."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        # Return results for File, Class, and Function nodes
        mock_result.__iter__.return_value = [
            {"id": "elem1", "qualifiedName": "test.py"},
            {"id": "elem2", "qualifiedName": "test.py::MyClass"},
            {"id": "elem3", "qualifiedName": "test.py::my_func"}
        ]
        mock_session.run.return_value = mock_result

        entities = [
            FileEntity(
                name="test.py",
                qualified_name="test.py",
                file_path="test.py",
                line_start=1,
                line_end=10,
                language="python",
                loc=10
            ),
            ClassEntity(
                name="MyClass",
                qualified_name="test.py::MyClass",
                file_path="test.py",
                line_start=1,
                line_end=5
            ),
            FunctionEntity(
                name="my_func",
                qualified_name="test.py::my_func",
                file_path="test.py",
                line_start=6,
                line_end=10
            )
        ]

        id_mapping = client.batch_create_nodes(entities)

        # Should have called run for each node type (File, Class, Function)
        assert mock_session.run.call_count >= 1
        assert len(id_mapping) == 3

    def test_batch_create_nodes_empty_list(self, client, mock_driver):
        """Test batch creating with empty list."""
        mock_session = mock_driver.session.return_value.__enter__.return_value

        id_mapping = client.batch_create_nodes([])

        assert len(id_mapping) == 0
        mock_session.run.assert_not_called()


class TestRelationshipOperations:
    """Test relationship creation."""

    def test_batch_create_relationships(self, client, mock_driver):
        """Test batch creating relationships."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_session.run.return_value = mock_result

        relationships = [
            Relationship(
                source_id="elem1",
                target_id="elem2",
                rel_type=RelationshipType.IMPORTS,
                properties={"line": 1}
            ),
            Relationship(
                source_id="elem2",
                target_id="elem3",
                rel_type=RelationshipType.CALLS,
                properties={"line": 5}
            )
        ]

        count = client.batch_create_relationships(relationships)

        assert count == 2
        # Should be called once per relationship type
        assert mock_session.run.call_count >= 1

    def test_batch_create_relationships_empty(self, client, mock_driver):
        """Test batch creating with empty relationships list."""
        mock_session = mock_driver.session.return_value.__enter__.return_value

        count = client.batch_create_relationships([])

        assert count == 0
        mock_session.run.assert_not_called()

    def test_batch_create_relationships_groups_by_type(self, client, mock_driver):
        """Test relationships are grouped by type for efficiency."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = []
        mock_session.run.return_value = mock_result

        relationships = [
            Relationship(
                source_id="elem1",
                target_id="elem2",
                rel_type=RelationshipType.IMPORTS,
                properties={}
            ),
            Relationship(
                source_id="elem3",
                target_id="elem4",
                rel_type=RelationshipType.IMPORTS,
                properties={}
            ),
            Relationship(
                source_id="elem5",
                target_id="elem6",
                rel_type=RelationshipType.CALLS,
                properties={}
            )
        ]

        client.batch_create_relationships(relationships)

        # Should group by type: 2 IMPORTS in one batch, 1 CALLS in another
        assert mock_session.run.call_count == 2


class TestUtilityMethods:
    """Test utility methods."""

    def test_clear_graph(self, client, mock_driver):
        """Test clearing all nodes from graph."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.__iter__.return_value = [{"deletedNodes": 100}]
        mock_session.run.return_value = mock_result

        client.clear_graph()

        mock_session.run.assert_called()
        # Verify DELETE query was executed
        call_args = mock_session.run.call_args[0][0]
        assert "DELETE" in call_args.upper() or "DETACH" in call_args.upper()

    def test_get_stats(self, client, mock_driver):
        """Test getting database statistics."""
        mock_session = mock_driver.session.return_value.__enter__.return_value

        # get_stats runs 5 queries, each returning [{"count": X}]
        # Setup side_effect to return different results for each query
        mock_results = [
            MagicMock(__iter__=lambda self: iter([{"count": 1000}])),  # total_nodes
            MagicMock(__iter__=lambda self: iter([{"count": 50}])),    # total_files
            MagicMock(__iter__=lambda self: iter([{"count": 200}])),   # total_classes
            MagicMock(__iter__=lambda self: iter([{"count": 750}])),   # total_functions
            MagicMock(__iter__=lambda self: iter([{"count": 1500}]))   # total_relationships
        ]
        mock_session.run.side_effect = mock_results

        stats = client.get_stats()

        assert stats["total_nodes"] == 1000
        assert stats["total_files"] == 50
        assert stats["total_classes"] == 200
        assert stats["total_functions"] == 750
        assert stats["total_relationships"] == 1500
        assert mock_session.run.call_count == 5


class TestErrorHandling:
    """Test error handling."""

    def test_connection_error_handling(self, mock_driver):
        """Test handling connection errors."""
        with patch('falkor.graph.client.GraphDatabase') as mock_gd:
            mock_gd.driver.side_effect = Exception("Connection failed")

            with pytest.raises(Exception) as exc_info:
                Neo4jClient(uri="bolt://invalid:7687")

            assert "Connection failed" in str(exc_info.value)

    def test_query_error_handling(self, client, mock_driver):
        """Test handling query execution errors."""
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_session.run.side_effect = Exception("Query failed")

        with pytest.raises(Exception) as exc_info:
            client.execute_query("INVALID CYPHER")

        assert "Query failed" in str(exc_info.value)
