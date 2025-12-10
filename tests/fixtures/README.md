# Test Fixtures

Minimal JSON fixtures for testing the STAC query script.

## Files

### `stac_source_collection.json`
Source collection with 3 items:
- `item-001`: New item to process
- `item-002`: Already exists in target (should be excluded)
- `item-003`: New item to process

### `stac_target_collection.json`
Target collection with already converted items:
- `item-002`: Already processed

## Usage

These fixtures are loaded by `load_fixtures()` in the test file and converted to STAC items using `create_stac_item()`. Tests that need special scenarios (like items without self links or error conditions) create items directly in the test using `create_stac_item()` for flexibility.
