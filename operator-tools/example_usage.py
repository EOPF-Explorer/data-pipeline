#!/usr/bin/env python3
"""
Example: Using the Refactored STAC Management Tools Programmatically

This script demonstrates how to use manage_item.py and manage_collections.py
as Python modules in your own code.
"""

import os

import boto3
from manage_collections import STACCollectionManager
from manage_item import STACItemManager, count_s3_objects_for_item, extract_s3_urls_from_item

# Configuration
API_URL = "https://api.explorer.eopf.copernicus.eu/stac"
COLLECTION_ID = "sentinel-2-l2a-staging"

# Initialize managers
item_manager = STACItemManager(API_URL)
collection_manager = STACCollectionManager(API_URL)

# Initialize S3 client (if needed)

endpoint = os.getenv("AWS_ENDPOINT_URL")
if endpoint is not None:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
else:
    s3_client = boto3.client("s3")


def example_1_inspect_single_item() -> None:
    """Example: Inspect a single item and its S3 data."""
    print("\n" + "=" * 60)
    print("Example 1: Inspecting a Single Item")
    print("=" * 60)

    item_id = "S2A_MSIL2A_20210917T115221_N0500_R123_T28RBS_20230110T165456"

    # Fetch the item
    item = item_manager.get_item(COLLECTION_ID, item_id)

    if item:
        print(f"✓ Found item: {item['id']}")
        print(f"  Collection: {item['collection']}")
        print(f"  Assets: {len(item.get('assets', {}))}")

        # Extract S3 URLs
        s3_urls = extract_s3_urls_from_item(item)
        print(f"  S3 URLs: {len(s3_urls)}")

        if s3_urls:
            # Count S3 objects
            obj_count = count_s3_objects_for_item(s3_client, s3_urls)
            print(f"  S3 Objects: {obj_count:,}")

            # Get detailed stats
            obj_count, size, urls = item_manager.get_item_s3_stats(item, s3_client)
            print(f"  Total Size: {size / (1024**3):.2f} GB")


def example_2_delete_single_item() -> None:
    """Example: Delete a single item with S3 cleanup."""
    print("\n" + "=" * 60)
    print("Example 2: Deleting a Single Item (DRY RUN)")
    print("=" * 60)

    item_id = "SOME_ITEM_ID"

    # First, fetch the item to see what we're working with
    item = item_manager.get_item(COLLECTION_ID, item_id)

    if not item:
        print(f"✗ Item {item_id} not found")
        return

    # Preview S3 data
    s3_urls = extract_s3_urls_from_item(item)
    if s3_urls:
        obj_count = count_s3_objects_for_item(s3_client, s3_urls)
        print(f"Would delete {obj_count:,} S3 objects")

    # Uncomment to actually delete (with validation):
    # success, s3_deleted, s3_failed = item_manager.delete_item(
    #     collection_id=COLLECTION_ID,
    #     item_id=item_id,
    #     clean_s3=True,
    #     s3_client=s3_client,
    #     item_dict=item,
    #     validate_s3=True,
    # )
    #
    # if success:
    #     print(f"✓ Deleted item and {s3_deleted:,} S3 objects")
    # else:
    #     print(f"✗ Deletion failed (S3 issues: {s3_failed})")


def example_3_process_collection_items() -> None:
    """Example: Process all items in a collection."""
    print("\n" + "=" * 60)
    print("Example 3: Processing Collection Items")
    print("=" * 60)

    # Get all items
    items = collection_manager.get_collection_items(COLLECTION_ID)
    print(f"Found {len(items)} items in collection")

    # Process each item
    items_with_s3 = 0
    total_objects = 0

    for item in items[:5]:  # Sample first 5 items
        s3_urls = extract_s3_urls_from_item(item)

        if s3_urls:
            items_with_s3 += 1
            obj_count = count_s3_objects_for_item(s3_client, s3_urls)
            total_objects += obj_count
            print(f"  {item['id']}: {obj_count:,} S3 objects")

    print("\nSummary:")
    print(f"  Items with S3 data: {items_with_s3}/{len(items[:5])}")
    print(f"  Total S3 objects: {total_objects:,}")


def example_4_clean_collection_with_filtering() -> None:
    """Example: Clean collection with custom filtering logic."""
    print("\n" + "=" * 60)
    print("Example 4: Selective Collection Cleaning")
    print("=" * 60)

    # Get all items
    items = collection_manager.get_collection_items(COLLECTION_ID)

    # Filter items based on custom criteria
    # Example: Only delete items older than a certain date
    from typing import Any

    items_to_delete: list[Any] = []
    for _item in items:
        # Add your filtering logic here
        # e.g., if item['properties']['datetime'] < some_date:
        pass

    print(f"Would delete {len(items_to_delete)} of {len(items)} items")

    # Delete filtered items (uncomment to execute)
    # deleted = 0
    # failed = 0
    # for item in items_to_delete:
    #     success, _, _ = item_manager.delete_item(
    #         collection_id=COLLECTION_ID,
    #         item_id=item['id'],
    #         clean_s3=True,
    #         s3_client=s3_client,
    #         item_dict=item,
    #         validate_s3=True,
    #     )
    #     if success:
    #         deleted += 1
    #     else:
    #         failed += 1
    #
    # print(f"✓ Deleted: {deleted}")
    # print(f"✗ Failed: {failed}")


def example_5_batch_s3_stats() -> None:
    """Example: Get S3 statistics for multiple items."""
    print("\n" + "=" * 60)
    print("Example 5: Batch S3 Statistics")
    print("=" * 60)

    items = collection_manager.get_collection_items(COLLECTION_ID)

    # Get stats for each item
    stats = []
    for item in items[:10]:  # Sample first 10
        obj_count, size, urls = item_manager.get_item_s3_stats(item, s3_client)
        stats.append(
            {
                "item_id": item["id"],
                "objects": obj_count,
                "size_gb": size / (1024**3),
                "url_count": len(urls),
            }
        )

    # Sort by size
    stats.sort(key=lambda x: x["size_gb"], reverse=True)

    print("\nTop items by size:")
    for stat in stats[:5]:
        print(f"  {stat['item_id']}")
        print(f"    Objects: {stat['objects']:,}")
        print(f"    Size: {stat['size_gb']:.2f} GB")


def example_6_error_handling() -> None:
    """Example: Proper error handling."""
    print("\n" + "=" * 60)
    print("Example 6: Error Handling")
    print("=" * 60)

    item_id = "NON_EXISTENT_ITEM"

    try:
        # This will return None for non-existent items
        item = item_manager.get_item(COLLECTION_ID, item_id)

        if item is None:
            print(f"✗ Item {item_id} not found")
            # Handle missing item case
            return

        # If we get here, item exists
        print(f"✓ Item found: {item['id']}")

        # Try S3 operations with error handling
        try:
            s3_urls = extract_s3_urls_from_item(item)
            if not s3_urls:
                print("⚠ No S3 URLs found in item")
                return

            obj_count = count_s3_objects_for_item(s3_client, s3_urls)
            print(f"✓ Found {obj_count:,} S3 objects")

        except Exception as e:
            print(f"✗ S3 operation failed: {e}")
            # Handle S3 errors

    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        # Handle other errors


def example_7_integration_with_existing_code() -> None:
    """Example: Integration pattern with existing code."""
    print("\n" + "=" * 60)
    print("Example 7: Integration Pattern")
    print("=" * 60)

    from typing import Any

    def my_custom_processing_function(item: Any) -> bool:
        """Your existing item processing logic."""
        # Do something with the item
        print(f"Processing: {item['id']}")
        return True

    # Integrate with STAC management
    items = collection_manager.get_collection_items(COLLECTION_ID)

    for item in items[:3]:
        # Run your custom processing
        if my_custom_processing_function(item):
            print("  ✓ Processed successfully")

            # Optionally check S3 data
            s3_urls = extract_s3_urls_from_item(item)
            if s3_urls:
                obj_count = count_s3_objects_for_item(s3_client, s3_urls)
                print(f"    S3 objects: {obj_count:,}")


def main() -> None:
    """Run all examples."""
    print("\n" + "=" * 60)
    print("STAC Management Tools - Usage Examples")
    print("=" * 60)
    print("\nNOTE: Most examples are in DRY RUN mode")
    print("Uncomment deletion code to actually execute")

    try:
        # Run examples
        example_1_inspect_single_item()
        # example_2_delete_single_item()  # Commented - requires valid item ID
        example_3_process_collection_items()
        # example_4_clean_collection_with_filtering()  # Commented - modifies data
        example_5_batch_s3_stats()
        example_6_error_handling()
        example_7_integration_with_existing_code()

    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 60)
    print("Examples completed")
    print("=" * 60)


if __name__ == "__main__":
    main()
