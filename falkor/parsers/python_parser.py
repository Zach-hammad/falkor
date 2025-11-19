"""Python code parser using AST module."""

import ast
from pathlib import Path
from typing import Any, List, Dict, Optional
import hashlib

from falkor.parsers.base import CodeParser
from falkor.models import (
    Entity,
    FileEntity,
    ModuleEntity,
    ClassEntity,
    FunctionEntity,
    Relationship,
    NodeType,
    RelationshipType,
)


class PythonParser(CodeParser):
    """Parser for Python source files."""

    def __init__(self) -> None:
        """Initialize Python parser."""
        self.file_entity: Optional[FileEntity] = None
        self.entity_map: Dict[str, str] = {}  # qualified_name -> entity_id

    def parse(self, file_path: str) -> ast.AST:
        """Parse Python file into AST.

        Args:
            file_path: Path to Python file

        Returns:
            Python AST
        """
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
            return ast.parse(source, filename=file_path)

    def extract_entities(self, tree: ast.AST, file_path: str) -> List[Entity]:
        """Extract entities from Python AST.

        Args:
            tree: Python AST
            file_path: Path to source file

        Returns:
            List of entities (File, Class, Function)
        """
        entities: List[Entity] = []

        # Create file entity
        file_entity = self._create_file_entity(file_path, tree)
        entities.append(file_entity)
        self.file_entity = file_entity

        # Extract classes and functions
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_entity = self._extract_class(node, file_path)
                entities.append(class_entity)

                # Extract methods from class
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        method_entity = self._extract_function(
                            item, file_path, class_name=node.name
                        )
                        entities.append(method_entity)

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Only top-level functions (not methods)
                if self._is_top_level(node, tree):
                    func_entity = self._extract_function(node, file_path)
                    entities.append(func_entity)

        # Extract module entities from imports
        module_entities = self._extract_modules(tree, file_path)
        entities.extend(module_entities)

        return entities

    def extract_relationships(
        self, tree: ast.AST, file_path: str, entities: List[Entity]
    ) -> List[Relationship]:
        """Extract relationships from Python AST.

        Args:
            tree: Python AST
            file_path: Path to source file
            entities: Extracted entities

        Returns:
            List of relationships (IMPORTS, CALLS, CONTAINS, etc.)
        """
        relationships: List[Relationship] = []

        # Build entity lookup map
        entity_map = {e.qualified_name: e for e in entities}

        # Extract imports (only module-level, not nested in functions/classes)
        file_entity_name = file_path  # Use file path as qualified name for File node

        # Use tree.body to only get module-level statements
        for node in tree.body:
            if isinstance(node, ast.Import):
                # Handle: import module [as alias]
                for alias in node.names:
                    module_name = alias.name
                    # Create IMPORTS relationship from File to module
                    relationships.append(
                        Relationship(
                            source_id=file_entity_name,
                            target_id=module_name,  # Will be mapped to Module node
                            rel_type=RelationshipType.IMPORTS,
                            properties={
                                "alias": alias.asname if alias.asname else None,
                                "line": node.lineno,
                            },
                        )
                    )

            elif isinstance(node, ast.ImportFrom):
                # Handle: from module import name [as alias]
                module_name = node.module or ""  # node.module can be None for "from . import"
                level = node.level  # Relative import level (0 = absolute, 1+ = relative)

                for alias in node.names:
                    imported_name = alias.name
                    # For "from foo import bar", create qualified name "foo.bar"
                    if module_name:
                        qualified_import = f"{module_name}.{imported_name}"
                    else:
                        qualified_import = imported_name

                    relationships.append(
                        Relationship(
                            source_id=file_entity_name,
                            target_id=qualified_import,
                            rel_type=RelationshipType.IMPORTS,
                            properties={
                                "alias": alias.asname if alias.asname else None,
                                "from_module": module_name,
                                "imported_name": imported_name,
                                "relative_level": level,
                                "line": node.lineno,
                            },
                        )
                    )

        # Extract function calls - need to track which function makes each call
        self._extract_calls(tree, file_path, entity_map, relationships)

        # Extract class inheritance relationships
        self._extract_inheritance(tree, file_path, relationships)

        # Extract method override relationships
        self._extract_overrides(tree, file_path, entity_map, relationships)

        # Create CONTAINS relationships
        file_qualified_name = file_path
        for entity in entities:
            if entity.node_type != NodeType.FILE:
                relationships.append(
                    Relationship(
                        source_id=file_qualified_name,
                        target_id=entity.qualified_name,
                        rel_type=RelationshipType.CONTAINS,
                    )
                )

        return relationships

    def _create_file_entity(self, file_path: str, tree: ast.AST) -> FileEntity:
        """Create file entity.

        Args:
            file_path: Path to file
            tree: AST

        Returns:
            FileEntity
        """
        path_obj = Path(file_path)

        # Calculate file hash
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        # Count lines of code
        with open(file_path, "r") as f:
            loc = len([line for line in f if line.strip()])

        return FileEntity(
            name=path_obj.name,
            qualified_name=file_path,
            file_path=file_path,
            line_start=1,
            line_end=loc,
            language="python",
            loc=loc,
            hash=file_hash,
        )

    def _extract_class(self, node: ast.ClassDef, file_path: str) -> ClassEntity:
        """Extract class entity from AST node.

        Args:
            node: ClassDef AST node
            file_path: Path to source file

        Returns:
            ClassEntity
        """
        qualified_name = f"{file_path}::{node.name}"
        docstring = ast.get_docstring(node)

        # Check if abstract
        is_abstract = any(
            isinstance(base, ast.Name) and base.id == "ABC" for base in node.bases
        )

        return ClassEntity(
            name=node.name,
            qualified_name=qualified_name,
            file_path=file_path,
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            docstring=docstring,
            is_abstract=is_abstract,
            complexity=self._calculate_complexity(node),
        )

    def _extract_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, file_path: str, class_name: Optional[str] = None
    ) -> FunctionEntity:
        """Extract function/method entity from AST node.

        Args:
            node: FunctionDef AST node
            file_path: Path to source file
            class_name: Parent class name if this is a method

        Returns:
            FunctionEntity
        """
        if class_name:
            qualified_name = f"{file_path}::{class_name}.{node.name}"
        else:
            qualified_name = f"{file_path}::{node.name}"

        docstring = ast.get_docstring(node)

        # Extract parameters
        parameters = [arg.arg for arg in node.args.args]

        # Extract return type if annotated
        return_type = None
        if node.returns:
            return_type = ast.unparse(node.returns)

        return FunctionEntity(
            name=node.name,
            qualified_name=qualified_name,
            file_path=file_path,
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            docstring=docstring,
            parameters=parameters,
            return_type=return_type,
            complexity=self._calculate_complexity(node),
            is_async=isinstance(node, ast.AsyncFunctionDef),
        )

    def _calculate_complexity(self, node: ast.AST) -> int:
        """Calculate cyclomatic complexity of a code block.

        Args:
            node: AST node

        Returns:
            Complexity score
        """
        complexity = 1  # Base complexity

        for child in ast.walk(node):
            # Each decision point adds 1 to complexity
            if isinstance(
                child,
                (
                    ast.If,
                    ast.While,
                    ast.For,
                    ast.ExceptHandler,
                    ast.With,
                    ast.Assert,
                    ast.BoolOp,
                ),
            ):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                # Each boolean operator adds complexity
                complexity += len(child.values) - 1

        return complexity

    def _is_top_level(self, node: ast.FunctionDef | ast.AsyncFunctionDef, tree: ast.AST) -> bool:
        """Check if function is top-level (not a method).

        Args:
            node: FunctionDef node
            tree: Full AST

        Returns:
            True if top-level function
        """
        # Simple heuristic: if function is in module body, it's top-level
        if hasattr(tree, "body"):
            return node in tree.body
        return False

    def _get_docstring(self, node: ast.AST) -> Optional[str]:
        """Extract docstring from AST node.

        Args:
            node: AST node

        Returns:
            Docstring or None
        """
        return ast.get_docstring(node)

    def _extract_calls(
        self,
        tree: ast.AST,
        file_path: str,
        entity_map: Dict[str, Entity],
        relationships: List[Relationship],
    ) -> None:
        """Extract function call relationships from AST.

        Args:
            tree: Python AST
            file_path: Path to source file
            entity_map: Map of qualified_name to Entity
            relationships: List to append relationships to
        """

        class CallVisitor(ast.NodeVisitor):
            """AST visitor to track function calls within their scope."""

            def __init__(self, file_path: str):
                self.file_path = file_path
                self.current_class: Optional[str] = None
                self.function_stack: List[str] = []  # Stack for nested functions
                self.calls: List[tuple[str, str, int]] = []  # (caller, callee, line)

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                """Visit class definition."""
                old_class = self.current_class
                self.current_class = node.name
                self.generic_visit(node)
                self.current_class = old_class

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                """Visit function definition."""
                self.function_stack.append(node.name)
                self.generic_visit(node)
                self.function_stack.pop()

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                """Visit async function definition."""
                self.function_stack.append(node.name)
                self.generic_visit(node)
                self.function_stack.pop()

            def visit_Call(self, node: ast.Call) -> None:
                """Visit function call."""
                if self.function_stack:
                    # Build function name from stack (handles nested functions)
                    func_name = ".".join(self.function_stack)

                    # Determine caller qualified name
                    if self.current_class:
                        caller = f"{self.file_path}::{self.current_class}.{func_name}"
                    else:
                        caller = f"{self.file_path}::{func_name}"

                    # Determine callee name (best effort)
                    callee = self._get_call_name(node)
                    if callee:
                        self.calls.append((caller, callee, node.lineno))

                self.generic_visit(node)

            def _get_call_name(self, node: ast.Call) -> Optional[str]:
                """Extract the name of what's being called.

                Args:
                    node: Call AST node

                Returns:
                    Called name or None
                """
                func = node.func
                if isinstance(func, ast.Name):
                    # Simple call: foo()
                    return func.id
                elif isinstance(func, ast.Attribute):
                    # Method call: obj.method()
                    # Try to build qualified name
                    parts = []
                    current = func
                    while isinstance(current, ast.Attribute):
                        parts.append(current.attr)
                        current = current.value
                    if isinstance(current, ast.Name):
                        parts.append(current.id)
                    return ".".join(reversed(parts))
                return None

        # Visit tree and collect calls
        visitor = CallVisitor(file_path)
        visitor.visit(tree)

        # Create CALLS relationships
        for caller, callee, line in visitor.calls:
            # Try to resolve callee to a qualified name in our entity map
            callee_qualified = None

            # Check if it's a direct reference to an entity in this file
            for qname, entity in entity_map.items():
                if entity.name == callee or qname.endswith(f"::{callee}"):
                    callee_qualified = qname
                    break

            # If not found, use the callee name as-is (might be external)
            if not callee_qualified:
                callee_qualified = callee

            relationships.append(
                Relationship(
                    source_id=caller,
                    target_id=callee_qualified,
                    rel_type=RelationshipType.CALLS,
                    properties={"line": line, "call_name": callee},
                )
            )

    def _extract_inheritance(
        self,
        tree: ast.AST,
        file_path: str,
        relationships: List[Relationship],
    ) -> None:
        """Extract class inheritance relationships from AST.

        Args:
            tree: Python AST
            file_path: Path to source file
            relationships: List to append relationships to
        """
        # Build a set of class names defined in this file
        local_classes = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                local_classes.add(node.name)

        # Now extract inheritance relationships
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                child_class_qualified = f"{file_path}::{node.name}"

                # Extract base classes
                for base in node.bases:
                    # Try to get the base class name
                    base_name = self._get_base_class_name(base)
                    if base_name:
                        # Determine the target qualified name
                        # If base class is defined in this file, use file-qualified name
                        # Otherwise, use the name as-is (could be simple or module.Class)
                        if base_name in local_classes:
                            # Intra-file inheritance
                            base_qualified = f"{file_path}::{base_name}"
                        else:
                            # Imported or external base class
                            # Use the name as extracted (e.g., "ABC", "typing.Generic", etc.)
                            base_qualified = base_name

                        relationships.append(
                            Relationship(
                                source_id=child_class_qualified,
                                target_id=base_qualified,
                                rel_type=RelationshipType.INHERITS,
                                properties={
                                    "base_class": base_name,
                                    "line": node.lineno,
                                },
                            )
                        )

    def _get_base_class_name(self, node: ast.expr) -> Optional[str]:
        """Extract base class name from AST node.

        Args:
            node: AST expression node representing base class

        Returns:
            Base class name or None
        """
        if isinstance(node, ast.Name):
            # Simple inheritance: class Foo(Bar)
            return node.id
        elif isinstance(node, ast.Attribute):
            # Qualified inheritance: class Foo(module.Bar)
            parts = []
            current = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return ".".join(reversed(parts))
        elif isinstance(node, ast.Subscript):
            # Generic inheritance: class Foo(Generic[T])
            # Extract the base type without the subscript
            return self._get_base_class_name(node.value)
        return None

    def _extract_modules(self, tree: ast.AST, file_path: str) -> List[ModuleEntity]:
        """Extract Module entities from import statements.

        Args:
            tree: Python AST
            file_path: Path to source file

        Returns:
            List of ModuleEntity objects
        """
        modules: Dict[str, ModuleEntity] = {}  # Deduplicate by qualified name

        # Only scan module-level imports
        for node in tree.body:
            if isinstance(node, ast.Import):
                # import foo, bar
                for alias in node.names:
                    module_name = alias.name
                    if module_name not in modules:
                        modules[module_name] = ModuleEntity(
                            name=module_name.split(".")[-1],  # Last component
                            qualified_name=module_name,
                            file_path=file_path,  # Source file that imports it
                            line_start=node.lineno,
                            line_end=node.lineno,
                            is_external=True,  # Assume external for now
                            package=self._get_package_name(module_name),
                        )

            elif isinstance(node, ast.ImportFrom):
                # from foo import bar
                module_name = node.module or ""  # Can be None for relative imports

                # Create module entity for the "from" module if it exists
                if module_name and module_name not in modules:
                    modules[module_name] = ModuleEntity(
                        name=module_name.split(".")[-1],
                        qualified_name=module_name,
                        file_path=file_path,
                        line_start=node.lineno,
                        line_end=node.lineno,
                        is_external=True,
                        package=self._get_package_name(module_name),
                    )

                # Also create entities for imported items if they look like modules
                # (e.g., "from typing import List" - List is not a module)
                # For now, we'll skip this and only create the parent module

        return list(modules.values())

    def _get_package_name(self, module_name: str) -> Optional[str]:
        """Extract parent package name from module name.

        Args:
            module_name: Fully qualified module name (e.g., "os.path")

        Returns:
            Parent package name (e.g., "os") or None
        """
        if "." in module_name:
            return module_name.rsplit(".", 1)[0]
        return None

    def _extract_overrides(
        self,
        tree: ast.AST,
        file_path: str,
        entity_map: Dict[str, Entity],
        relationships: List[Relationship],
    ) -> None:
        """Extract method override relationships from AST.

        Detects when a method in a child class overrides a method in a parent class.
        Only works for intra-file inheritance (both classes in same file).

        Args:
            tree: Python AST
            file_path: Path to source file
            entity_map: Map of qualified_name to Entity
            relationships: List to append relationships to
        """
        # Build a map of class_name -> class_node -> methods
        class_info: Dict[str, tuple[ast.ClassDef, Dict[str, str]]] = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Extract method names for this class
                methods: Dict[str, str] = {}  # method_name -> qualified_name
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        method_qualified = f"{file_path}::{node.name}.{item.name}"
                        methods[item.name] = method_qualified

                class_info[node.name] = (node, methods)

        # Now check for overrides
        for class_name, (class_node, child_methods) in class_info.items():
            # Check each base class
            for base in class_node.bases:
                base_name = self._get_base_class_name(base)
                if not base_name:
                    continue

                # Check if base class is defined in this file
                if base_name in class_info:
                    parent_node, parent_methods = class_info[base_name]

                    # Check for method overrides
                    for method_name, child_method_qualified in child_methods.items():
                        if method_name in parent_methods:
                            # Found an override!
                            parent_method_qualified = parent_methods[method_name]

                            # Skip special methods like __init__ (common but not interesting)
                            if method_name.startswith("__") and method_name.endswith("__"):
                                continue

                            relationships.append(
                                Relationship(
                                    source_id=child_method_qualified,
                                    target_id=parent_method_qualified,
                                    rel_type=RelationshipType.OVERRIDES,
                                    properties={
                                        "method_name": method_name,
                                        "child_class": class_name,
                                        "parent_class": base_name,
                                    },
                                )
                            )
