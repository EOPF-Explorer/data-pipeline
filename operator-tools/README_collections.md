# Collection Management Tool

A comprehensive CLI tool for managing STAC collections in the EOPF Explorer catalog using the Transaction API.

## Features

- **Clean Collections**: Remove all items from a collection
- **Create Collections**: Create new collections from JSON templates
- **Update Collections**: Update existing collection metadata
- **Batch Operations**: Process multiple collection templates at once
- **Collection Info**: View collection details and item counts

## Installation

The tool uses dependencies already included in the project. Ensure you have the environment set up:

```bash
# Install dependencies (if not already done)
uv sync
```

## Usage

### Basic Syntax

```bash
uv run operator-tools/manage_collections.py [OPTIONS] COMMAND [ARGS]
```

### Available Commands

#### 1. `clean` - Remove All Items from a Collection

Remove all items from a collection (useful for clearing test data or resetting a collection).

```bash
# Dry run (see what would be deleted without actually deleting)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run

# Actually clean the collection (will prompt for confirmation)
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging

# Skip confirmation prompt
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging -y
```

**Options:**
- `--dry-run`: Show what would be deleted without actually deleting
- `--yes, -y`: Skip confirmation prompt

**Safety Features:**
- Confirmation prompt before deletion (unless `--yes` is used)
- Progress bar showing deletion progress
- Handles missing items gracefully
- Reports success/failure counts

#### 2. `create` - Create or Update a Collection

Create a new collection or update an existing one from a JSON template file.

```bash
# Create a new collection
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json

# Update an existing collection
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update
```

**Options:**
- `--update`: Update existing collection instead of creating new

**Template Format:**
Templates must be valid STAC Collection JSON files with at minimum:
- `id`: Collection identifier
- `type`: Must be "Collection"
- Other standard STAC Collection fields (title, description, extent, etc.)

See `stac/sentinel-2-l2a.json` for an example.

#### 3. `batch-create` - Batch Create/Update Collections

Process multiple collection templates from a directory at once.

```bash
# Create all collections from templates in stac/ directory
uv run operator-tools/manage_collections.py batch-create stac/

# Update all collections
uv run operator-tools/manage_collections.py batch-create stac/ --update

# Use custom file pattern
uv run operator-tools/manage_collections.py batch-create stac/ --pattern "*-staging.json"
```

**Options:**
- `--update`: Update existing collections instead of creating new
- `--pattern`: File pattern to match (default: `*.json`)

**Features:**
- Processes all matching JSON files in directory
- Shows preview before proceeding
- Reports success/failure for each file
- Summary statistics at the end

#### 4. `info` - Show Collection Information

Display detailed information about a collection, including item count.

```bash
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging
```

**Output includes:**
- Collection ID and title
- Description
- License
- Item count
- Spatial and temporal extents

### Global Options

#### `--api-url`

Override the default STAC API URL:

```bash
uv run operator-tools/manage_collections.py --api-url https://custom.stac.api/stac info my-collection
```

**Default:** `https://api.explorer.eopf.copernicus.eu/stac`

## Common Workflows

### Create a New Collection

1. Create or edit a JSON template in the `stac/` directory
2. Run the create command:
   ```bash
   uv run operator-tools/manage_collections.py create stac/my-collection.json
   ```
3. Verify the collection was created:
   ```bash
   uv run operator-tools/manage_collections.py info my-collection
   ```

### Clean Up Test Data

1. Check what would be deleted:
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
   ```
2. If satisfied, proceed with deletion:
   ```bash
   uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging
   ```

### Update Collection Metadata

1. Edit the collection template in `stac/`
2. Update the collection:
   ```bash
   uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update
   ```

### Bulk Collection Setup

When setting up multiple collections from templates:

```bash
# Review templates in stac/ directory
ls stac/*.json

# Create all collections at once
uv run operator-tools/manage_collections.py batch-create stac/
```

## Transaction API Endpoints

The tool uses the following STAC Transaction API endpoints:

- `GET /collections/{collection_id}/items` - List items (for cleaning)
- `DELETE /collections/{collection_id}/items/{item_id}` - Delete item
- `POST /collections` - Create collection
- `PUT /collections` - Update collection

## Error Handling

The tool includes comprehensive error handling:

- **404 Errors**: Gracefully handles missing items/collections
- **Validation**: Validates JSON templates before submission
- **Network Errors**: Reports connection issues clearly
- **Partial Failures**: In batch operations, continues processing remaining items even if some fail

## Best Practices

1. **Always use `--dry-run` first** when cleaning collections to verify what will be deleted
2. **Test with staging collections** before operating on production collections
3. **Keep collection templates in version control** (in the `stac/` directory)
4. **Verify collection info** after create/update operations
5. **Use batch operations** for consistency when managing multiple collections

## Troubleshooting

### Connection Refused / Network Errors

- Verify the API URL is correct
- Check network connectivity to the STAC API
- Ensure you have necessary permissions

### Collection Not Found

```bash
# List all collections to find the correct ID
uv run operator-tools/manage_collections.py --help
```

### Invalid JSON Template

- Validate JSON syntax: `python -m json.tool < stac/template.json`
- Ensure required fields (`id`, `type`) are present
- Check that `type` is set to "Collection"

### Permission Denied

- Verify you have write access to the STAC API
- Check authentication credentials (if required)

## Examples

### Complete Collection Lifecycle

```bash
# 1. Create a new collection from template
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json

# 2. Check collection info
uv run operator-tools/manage_collections.py info sentinel-2-l2a-staging

# 3. (After testing) Clean test items
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging --dry-run
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-staging -y

# 4. Update collection metadata
uv run operator-tools/manage_collections.py create stac/sentinel-2-l2a.json --update
```

### Development Workflow

```bash
# Set up all collections from templates
uv run operator-tools/manage_collections.py batch-create stac/

# Clean specific test collection
uv run operator-tools/manage_collections.py clean sentinel-2-l2a-dp-test -y

# Update all collections with latest templates
uv run operator-tools/manage_collections.py batch-create stac/ --update
```

## Support

For issues or questions:
- Check the main [operator-tools README](README.md)
- Review STAC Transaction API documentation
- Contact the EOPF Explorer operations team

## Related Tools

- `submit_test_workflow_wh.py` - Submit STAC items for processing
- `submit_stac_items_notebook.ipynb` - Interactive batch item submission
