# Test Fixtures for Storage Tier Update Tests

This directory contains JSON fixtures used by `test_update_stac_storage_tier.py`.

## Fixtures

- **stac_item_before.json**: STAC item with existing `alternate.s3` (STANDARD tier)
- **stac_item_legacy.json**: Legacy STAC item without `alternate.s3`
- **stac_item_after_tier_change.json**: Expected result after tier change to STANDARD_IA
- **stac_item_mixed_storage.json**: STAC item with mixed storage tier distribution
- **s3_storage_responses.json**: Mock S3 storage tier responses for different URLs

## Usage

Tests use pytest fixtures that load these JSON files:

```python
@pytest.fixture
def stac_item_before():
    """STAC item with existing alternate.s3 (STANDARD tier)."""
    with open(FIXTURES_DIR / "stac_item_before.json") as f:
        return Item.from_dict(json.load(f))
```

This approach makes tests:
- **Readable**: Test logic separated from test data
- **Maintainable**: Update fixtures without touching test code
- **Reusable**: Same fixtures across multiple tests
