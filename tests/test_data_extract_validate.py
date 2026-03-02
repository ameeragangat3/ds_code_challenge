"""Unit tests for schema validation utilities."""

import copy
import unittest
from typing import Any

from src import HEX_SCHEMA
from src.data_extract_validate import validate_hex_schema


def _valid_geojson() -> dict[str, Any]:
    """Build a minimal valid GeoJSON feature collection for tests."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [18.0, -33.0],
                            [18.1, -33.0],
                            [18.1, -33.1],
                            [18.0, -33.0],
                        ]
                    ],
                },
                "properties": {
                    "resolution": 8,
                    "index": "881f146257fffff",
                },
            }
        ],
    }


class TestHexSchemaValidation(unittest.TestCase):
    """Tests for schema rule evaluation and conformance scoring."""

    def test_validate_hex_schema_valid_payload_scores_one(self) -> None:
        """Valid payload should pass all checks with conformance score 1.0."""
        report = validate_hex_schema(_valid_geojson(), HEX_SCHEMA)

        self.assertEqual(report["total_features"], 1)
        self.assertEqual(report["total_rule_failures"], 0)
        self.assertEqual(report["conformance_score"], 1.0)
        self.assertEqual(report["failure_breakdown"], {})

    def test_validate_hex_schema_invalid_payload_reports_failures(self) -> None:
        """Invalid values should be reflected in failure breakdown counts."""
        invalid = copy.deepcopy(_valid_geojson())
        invalid["features"][0]["properties"]["resolution"] = 9
        invalid["features"][0]["properties"]["index"] = 123

        report = validate_hex_schema(invalid, HEX_SCHEMA)

        self.assertLess(report["conformance_score"], 1.0)
        self.assertGreaterEqual(report["failure_breakdown"]["resolution_value_invalid"], 1)
        self.assertGreaterEqual(report["failure_breakdown"]["index_type_invalid"], 1)


if __name__ == "__main__":
    unittest.main()
