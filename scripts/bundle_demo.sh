#!/bin/bash
set -e

# Bundle script for the Knowledge Graph Demo
# Builds search_tui and graph_pipeline release binaries

DIST_DIR="dist"
echo "--- Initializing $DIST_DIR ---"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

echo "--- Building release binaries ---"
cargo build --example search_tui --example graph_pipeline --release

echo "--- Copying binaries to $DIST_DIR ---"
cp target/release/examples/search_tui "$DIST_DIR/"
cp target/release/examples/graph_pipeline "$DIST_DIR/"

echo "--- Bundle Complete ---"
echo "Success! The bundled demo is available in: $DIST_DIR/"
echo ""
echo "Steps to test:"
echo "1. Run the graph pipeline to ingest a URL:"
echo "   cd $DIST_DIR && ./graph_pipeline https://example.com"
echo "2. Once complete, run the search TUI (which loads graph_state.json):"
echo "   cd $DIST_DIR && ./search_tui"
echo ""
ls -lh "$DIST_DIR"
