"""Unit tests for thumbnail cache warming functionality."""

from unittest.mock import MagicMock, patch

import pytest
from pystac import Asset, Item

from scripts.register_v1 import warm_thumbnail_cache


class TestWarmThumbnailCache:
    """Tests for warm_thumbnail_cache function."""

    @pytest.fixture
    def stac_item_with_thumbnail(self):
        """Create STAC item with thumbnail asset."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "thumbnail",
            Asset(
                href="https://api.explorer.eopf.copernicus.eu/raster/collections/sentinel-2-l2a/items/test-item/preview?format=png&...",
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )
        return item

    @pytest.fixture
    def stac_item_without_thumbnail(self):
        """Create STAC item without thumbnail asset."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        return item

    @patch("scripts.register_v1.httpx.Client")
    def test_successful_cache_warming(self, mock_client, stac_item_with_thumbnail):
        """Test successful thumbnail cache warming."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.content = b"fake_image_data" * 1000  # ~13KB
        mock_client.return_value.__enter__.return_value.get.return_value = mock_response

        # Should not raise
        warm_thumbnail_cache(stac_item_with_thumbnail)

        # Verify HTTP request was made
        mock_client.return_value.__enter__.return_value.get.assert_called_once()
        call_args = mock_client.return_value.__enter__.return_value.get.call_args
        assert "preview" in call_args[0][0]

    @patch("scripts.register_v1.httpx.Client")
    def test_no_thumbnail_asset(self, mock_client, stac_item_without_thumbnail):
        """Test graceful handling when thumbnail asset doesn't exist."""
        warm_thumbnail_cache(stac_item_without_thumbnail)

        # Should not make HTTP request
        mock_client.return_value.__enter__.return_value.get.assert_not_called()

    @patch("scripts.register_v1.httpx.Client")
    def test_thumbnail_with_empty_href(self, mock_client):
        """Test handling thumbnail asset with empty href."""
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        # Thumbnail with empty href
        item.add_asset(
            "thumbnail",
            Asset(
                href="",  # Empty string
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )

        warm_thumbnail_cache(item)

        # Should not make HTTP request
        mock_client.return_value.__enter__.return_value.get.assert_not_called()

    @patch("scripts.register_v1.httpx.Client")
    def test_http_timeout_handling(self, mock_client, stac_item_with_thumbnail):
        """Test graceful handling of HTTP timeout."""
        import httpx

        mock_client.return_value.__enter__.return_value.get.side_effect = httpx.TimeoutException(
            "Request timed out"
        )

        # Should not raise - errors are logged and swallowed
        warm_thumbnail_cache(stac_item_with_thumbnail)

    @patch("scripts.register_v1.httpx.Client")
    def test_http_error_handling(self, mock_client, stac_item_with_thumbnail):
        """Test graceful handling of HTTP errors (404, 500, etc.)."""
        import httpx

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=MagicMock(), response=MagicMock()
        )
        mock_client.return_value.__enter__.return_value.get.return_value = mock_response

        # Should not raise
        warm_thumbnail_cache(stac_item_with_thumbnail)

    @patch("scripts.register_v1.httpx.Client")
    def test_generic_exception_handling(self, mock_client, stac_item_with_thumbnail):
        """Test graceful handling of unexpected exceptions."""
        mock_client.return_value.__enter__.return_value.get.side_effect = Exception(
            "Unexpected error"
        )

        # Should not raise
        warm_thumbnail_cache(stac_item_with_thumbnail)

    @patch("scripts.register_v1.httpx.Client")
    def test_uses_correct_timeout(self, mock_client, stac_item_with_thumbnail):
        """Test that HTTP client is configured with 60s timeout."""
        warm_thumbnail_cache(stac_item_with_thumbnail)

        # Verify Client was instantiated with timeout=60.0
        mock_client.assert_called_once_with(timeout=60.0, follow_redirects=True)

    @patch("scripts.register_v1.httpx.Client")
    def test_follows_redirects(self, mock_client, stac_item_with_thumbnail):
        """Test that HTTP client follows redirects."""
        warm_thumbnail_cache(stac_item_with_thumbnail)

        # Verify follow_redirects=True
        mock_client.assert_called_once_with(timeout=60.0, follow_redirects=True)


class TestCacheWarmingIntegration:
    """Integration tests to verify cache warming is called in registration workflow."""

    def test_cache_warming_called_in_correct_order(self):
        """Test that warm_thumbnail_cache is called after add_thumbnail_asset."""
        # This test verifies the call order by inspecting the source code structure
        # Rather than running the full registration workflow
        import inspect

        from scripts import register_v1

        # Get the source code of run_registration
        source = inspect.getsource(register_v1.run_registration)

        # Find the positions of the function calls
        add_thumbnail_pos = source.find("add_thumbnail_asset(")
        warm_cache_pos = source.find("warm_thumbnail_cache(")

        # Verify both functions are called
        assert add_thumbnail_pos > 0, "add_thumbnail_asset should be called in run_registration"
        assert warm_cache_pos > 0, "warm_thumbnail_cache should be called in run_registration"

        # Verify cache warming comes after thumbnail creation
        assert (
            warm_cache_pos > add_thumbnail_pos
        ), "warm_thumbnail_cache should be called after add_thumbnail_asset"

    @patch("scripts.register_v1.logger")
    def test_cache_warming_errors_are_logged_not_raised(self, mock_logger):
        """Test that cache warming errors are logged but don't stop execution."""
        import httpx

        from scripts.register_v1 import warm_thumbnail_cache

        # Create item with thumbnail
        item = Item(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            datetime="2025-01-01T00:00:00Z",
            properties={},
        )
        item.add_asset(
            "thumbnail",
            Asset(
                href="https://api.example.com/thumbnail.png",
                media_type="image/png",
                roles=["thumbnail"],
            ),
        )

        # Mock httpx to raise an error
        with patch("scripts.register_v1.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = (
                httpx.TimeoutException("Timeout")
            )

            # Should not raise exception
            warm_thumbnail_cache(item)

            # Should log a warning
            assert mock_logger.warning.called, "Should log warning when cache warming fails"
            warning_msg = str(mock_logger.warning.call_args)
            assert "Cache warming" in warning_msg or "timed out" in warning_msg.lower()

    def test_warm_thumbnail_cache_function_signature(self):
        """Test that warm_thumbnail_cache has the expected function signature."""
        import inspect

        from scripts.register_v1 import warm_thumbnail_cache

        sig = inspect.signature(warm_thumbnail_cache)
        params = list(sig.parameters.keys())

        # Should take exactly one parameter: item
        assert len(params) == 1, "warm_thumbnail_cache should take exactly 1 parameter"
        assert params[0] == "item", "Parameter should be named 'item'"

        # Check return annotation - it can be None, 'None' (string), empty, or NoneType
        return_annotation = sig.return_annotation
        valid_return_types = [None, inspect.Signature.empty, type(None), "None"]

        # For string annotations, also check if it's the string 'None'
        assert (
            return_annotation in valid_return_types or str(return_annotation) == "None"
        ), f"Expected return annotation to be None-like, got {return_annotation}"

    def test_warm_thumbnail_cache_is_imported(self):
        """Test that warm_thumbnail_cache is available in the module."""
        from scripts import register_v1

        assert hasattr(
            register_v1, "warm_thumbnail_cache"
        ), "warm_thumbnail_cache should be defined in register_v1"
        assert callable(register_v1.warm_thumbnail_cache), "warm_thumbnail_cache should be callable"
