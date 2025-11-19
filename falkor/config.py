"""Configuration management for Falkor.

Supports loading configuration from:
- .falkorrc (YAML or JSON format)
- falkor.toml (TOML format)

Configuration is searched hierarchically:
1. Current directory
2. Parent directories (up to root)
3. User home directory (~/.falkorrc or ~/.config/falkor.toml)

Example .falkorrc (YAML):
```yaml
neo4j:
  uri: bolt://localhost:7687
  user: neo4j
  password: ${NEO4J_PASSWORD}

ingestion:
  patterns:
    - "**/*.py"
    - "**/*.js"
  follow_symlinks: false
  max_file_size_mb: 10
  batch_size: 100

analysis:
  min_modularity: 0.3
  max_coupling: 5.0

logging:
  level: INFO
  format: human
  file: logs/falkor.log
```

Example falkor.toml:
```toml
[neo4j]
uri = "bolt://localhost:7687"
user = "neo4j"
password = "${NEO4J_PASSWORD}"

[ingestion]
patterns = ["**/*.py", "**/*.js"]
follow_symlinks = false
max_file_size_mb = 10
batch_size = 100

[analysis]
min_modularity = 0.3
max_coupling = 5.0

[logging]
level = "INFO"
format = "human"
file = "logs/falkor.log"
```
"""

import os
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import tomli
    HAS_TOML = True
except ImportError:
    try:
        import tomllib as tomli  # Python 3.11+
        HAS_TOML = True
    except ImportError:
        HAS_TOML = False

from falkor.logging_config import get_logger

logger = get_logger(__name__)


class ConfigError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""
    pass


@dataclass
class Neo4jConfig:
    """Neo4j connection configuration."""
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: Optional[str] = None


@dataclass
class IngestionConfig:
    """Ingestion pipeline configuration."""
    patterns: list[str] = field(default_factory=lambda: ["**/*.py"])
    follow_symlinks: bool = False
    max_file_size_mb: float = 10.0
    batch_size: int = 100


@dataclass
class AnalysisConfig:
    """Analysis engine configuration."""
    min_modularity: float = 0.3
    max_coupling: float = 5.0


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "human"  # "human" or "json"
    file: Optional[str] = None


@dataclass
class FalkorConfig:
    """Complete Falkor configuration."""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FalkorConfig":
        """Create config from dictionary.

        Args:
            data: Configuration dictionary

        Returns:
            FalkorConfig instance
        """
        # Expand environment variables
        data = _expand_env_vars(data)

        return cls(
            neo4j=Neo4jConfig(**data.get("neo4j", {})),
            ingestion=IngestionConfig(**data.get("ingestion", {})),
            analysis=AnalysisConfig(**data.get("analysis", {})),
            logging=LoggingConfig(**data.get("logging", {})),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary.

        Returns:
            Configuration as dictionary
        """
        return {
            "neo4j": {
                "uri": self.neo4j.uri,
                "user": self.neo4j.user,
                "password": self.neo4j.password,
            },
            "ingestion": {
                "patterns": self.ingestion.patterns,
                "follow_symlinks": self.ingestion.follow_symlinks,
                "max_file_size_mb": self.ingestion.max_file_size_mb,
                "batch_size": self.ingestion.batch_size,
            },
            "analysis": {
                "min_modularity": self.analysis.min_modularity,
                "max_coupling": self.analysis.max_coupling,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "file": self.logging.file,
            },
        }

    def merge(self, other: "FalkorConfig") -> "FalkorConfig":
        """Merge with another config (other takes precedence).

        Args:
            other: Config to merge with

        Returns:
            New merged config
        """
        merged_dict = self.to_dict()
        other_dict = other.to_dict()

        # Deep merge
        for section, values in other_dict.items():
            if section not in merged_dict:
                merged_dict[section] = values
            else:
                merged_dict[section].update(values)

        return FalkorConfig.from_dict(merged_dict)


def _expand_env_vars(data: Union[Dict, list, str, Any]) -> Any:
    """Recursively expand environment variables in config data.

    Supports ${VAR_NAME} and $VAR_NAME syntax.

    Args:
        data: Configuration data (dict, list, str, or primitive)

    Returns:
        Data with environment variables expanded
    """
    if isinstance(data, dict):
        return {k: _expand_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_expand_env_vars(item) for item in data]
    elif isinstance(data, str):
        # Match ${VAR} or $VAR
        pattern = re.compile(r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)')

        def replace_var(match):
            var_name = match.group(1) or match.group(2)
            return os.environ.get(var_name, match.group(0))

        return pattern.sub(replace_var, data)
    else:
        return data


def find_config_file(start_dir: Optional[Path] = None) -> Optional[Path]:
    """Find config file using hierarchical search.

    Searches in order:
    1. start_dir (or current directory)
    2. Parent directories up to root
    3. User home directory

    Looks for (in order of preference):
    - .falkorrc (YAML/JSON)
    - falkor.toml

    Args:
        start_dir: Starting directory for search (default: current directory)

    Returns:
        Path to config file, or None if not found
    """
    if start_dir is None:
        start_dir = Path.cwd()
    else:
        start_dir = Path(start_dir).resolve()

    # Search current directory and parents
    current = start_dir
    while True:
        # Check for .falkorrc
        falkorrc = current / ".falkorrc"
        if falkorrc.exists() and falkorrc.is_file():
            logger.info(f"Found config file: {falkorrc}")
            return falkorrc

        # Check for falkor.toml
        falkor_toml = current / "falkor.toml"
        if falkor_toml.exists() and falkor_toml.is_file():
            logger.info(f"Found config file: {falkor_toml}")
            return falkor_toml

        # Move to parent
        parent = current.parent
        if parent == current:  # Reached root
            break
        current = parent

    # Check home directory
    home = Path.home()

    # Check ~/.falkorrc
    home_falkorrc = home / ".falkorrc"
    if home_falkorrc.exists() and home_falkorrc.is_file():
        logger.info(f"Found config file: {home_falkorrc}")
        return home_falkorrc

    # Check ~/.config/falkor.toml
    config_dir = home / ".config"
    config_toml = config_dir / "falkor.toml"
    if config_toml.exists() and config_toml.is_file():
        logger.info(f"Found config file: {config_toml}")
        return config_toml

    logger.debug("No config file found")
    return None


def load_config_file(file_path: Path) -> Dict[str, Any]:
    """Load configuration from file.

    Supports:
    - .falkorrc (YAML or JSON)
    - falkor.toml (TOML)

    Args:
        file_path: Path to config file

    Returns:
        Configuration dictionary

    Raises:
        ConfigError: If file cannot be parsed or format not supported
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise ConfigError(f"Config file not found: {file_path}")

    try:
        content = file_path.read_text()
    except Exception as e:
        raise ConfigError(f"Failed to read config file {file_path}: {e}")

    # Detect format and parse
    if file_path.name == ".falkorrc" or file_path.suffix in [".yaml", ".yml", ".json"]:
        # Try YAML first (if available and appropriate extension)
        if HAS_YAML and file_path.suffix in [".yaml", ".yml", ""]:
            try:
                data = yaml.safe_load(content)
                logger.debug(f"Loaded YAML config from {file_path}")
                return data or {}
            except yaml.YAMLError:
                pass  # Try JSON

        # Try JSON
        try:
            data = json.loads(content)
            logger.debug(f"Loaded JSON config from {file_path}")
            return data
        except json.JSONDecodeError as e:
            raise ConfigError(
                f"Failed to parse {file_path} as YAML or JSON: {e}\n"
                f"Install PyYAML for YAML support: pip install pyyaml"
            )

    elif file_path.suffix == ".toml":
        if not HAS_TOML:
            raise ConfigError(
                f"TOML support not available. Install tomli: pip install tomli"
            )

        try:
            data = tomli.loads(content)
            logger.debug(f"Loaded TOML config from {file_path}")
            return data
        except Exception as e:
            raise ConfigError(f"Failed to parse TOML config {file_path}: {e}")

    else:
        raise ConfigError(f"Unsupported config file format: {file_path}")


def load_config(
    config_file: Optional[Union[str, Path]] = None,
    search_path: Optional[Path] = None,
) -> FalkorConfig:
    """Load Falkor configuration.

    If config_file is specified, loads from that file.
    Otherwise, searches hierarchically for config file.

    Args:
        config_file: Explicit path to config file (optional)
        search_path: Starting directory for hierarchical search (default: current dir)

    Returns:
        FalkorConfig instance

    Raises:
        ConfigError: If specified config file cannot be loaded
    """
    if config_file:
        # Explicit config file specified
        config_path = Path(config_file)
        data = load_config_file(config_path)
        logger.info(f"Loaded configuration from {config_path}")
        return FalkorConfig.from_dict(data)

    # Search for config file
    config_path = find_config_file(search_path)

    if config_path:
        data = load_config_file(config_path)
        logger.info(f"Loaded configuration from {config_path}")
        return FalkorConfig.from_dict(data)

    # No config file found, use defaults
    logger.info("No config file found, using defaults")
    return FalkorConfig()


def generate_config_template(format: str = "yaml") -> str:
    """Generate configuration file template.

    Args:
        format: Template format ("yaml", "json", or "toml")

    Returns:
        Configuration template as string

    Raises:
        ValueError: If format is not supported
    """
    config = FalkorConfig()
    data = config.to_dict()

    if format == "yaml":
        if not HAS_YAML:
            raise ConfigError("YAML support not available. Install: pip install pyyaml")

        template = yaml.dump(data, default_flow_style=False, sort_keys=False)
        return f"""# Falkor Configuration File (.falkorrc)
#
# This file configures Falkor's behavior. It can be placed:
# - In your project root: .falkorrc
# - In your home directory: ~/.falkorrc
# - In your config directory: ~/.config/falkor.toml
#
# Environment variables can be referenced using ${VAR_NAME} syntax.

{template}"""

    elif format == "json":
        # Add comments as special keys (JSON doesn't support real comments)
        commented_data = {
            "_comment": "Falkor Configuration File (.falkorrc)",
            "_note": "Environment variables can be referenced using ${VAR_NAME} syntax",
        }
        commented_data.update(data)
        template = json.dumps(commented_data, indent=2)
        return template

    elif format == "toml":
        if not HAS_TOML:
            raise ConfigError("TOML support not available. Install: pip install tomli")

        # Manual TOML generation (tomli doesn't have dump)
        lines = [
            "# Falkor Configuration File (falkor.toml)",
            "#",
            "# This file configures Falkor's behavior. It can be placed:",
            "# - In your project root: falkor.toml",
            "# - In your home directory: ~/.config/falkor.toml",
            "#",
            "# Environment variables can be referenced using ${VAR_NAME} syntax.",
            "",
            "[neo4j]",
            f'uri = "{data["neo4j"]["uri"]}"',
            f'user = "{data["neo4j"]["user"]}"',
            f'password = "{data["neo4j"]["password"] or ""}"',
            "",
            "[ingestion]",
            f'patterns = {json.dumps(data["ingestion"]["patterns"])}',
            f'follow_symlinks = {str(data["ingestion"]["follow_symlinks"]).lower()}',
            f'max_file_size_mb = {data["ingestion"]["max_file_size_mb"]}',
            f'batch_size = {data["ingestion"]["batch_size"]}',
            "",
            "[analysis]",
            f'min_modularity = {data["analysis"]["min_modularity"]}',
            f'max_coupling = {data["analysis"]["max_coupling"]}',
            "",
            "[logging]",
            f'level = "{data["logging"]["level"]}"',
            f'format = "{data["logging"]["format"]}"',
            f'file = "{data["logging"]["file"] or ""}"',
        ]

        return "\n".join(lines)

    else:
        raise ValueError(f"Unsupported format: {format}. Use 'yaml', 'json', or 'toml'")
