`zarr.json` of the root and both orbit groups of `s1-rtc-32UPB.zarr`, a healthy cube in
`sentinel-1-grd-rtc-staging`. They were read through the gateway on 30 Sep 2026, and all three carry a
`consolidated_metadata` block written by the S1 ingest.

`test_consolidate_zarr_groups.py` rebuilds every node of the cube from the root's block, strips the
blocks to match the 30 stripped cubes in #446, runs the repair, and expects these three bodies back.
