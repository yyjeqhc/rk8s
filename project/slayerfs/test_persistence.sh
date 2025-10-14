#!/bin/bash
set -e

DEMO_BIN="/root/git/rk8s/project/target/debug/examples/persistence_demo"
CONFIG_FILE="/root/git/rk8s/project/slayerfs/sqlite.yml"
MOUNT_POINT="/tmp/slayerfs_test_mount"
STORAGE_DIR="/tmp/slayerfs_test_storage"

echo "=== SlayerFS Persistence Test ==="
echo ""

# Clean up previous test
echo "1. Cleaning up previous test..."
fusermount -u "$MOUNT_POINT" 2>/dev/null || true
rm -rf "$MOUNT_POINT" "$STORAGE_DIR"
mkdir -p "$MOUNT_POINT" "$STORAGE_DIR"

# Start the filesystem in background
echo "2. Starting SlayerFS in background..."
$DEMO_BIN --config "$CONFIG_FILE" --mount "$MOUNT_POINT" --storage "$STORAGE_DIR" &
DEMO_PID=$!

# Wait for mount
echo "3. Waiting for filesystem to mount..."
sleep 2

# Check if mounted
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "❌ Failed to mount filesystem"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi
echo "✅ Filesystem mounted successfully"

# Test basic operations
echo ""
echo "4. Testing basic operations..."

# Test mkdir
echo "   - Creating directory /test_dir..."
mkdir "$MOUNT_POINT/test_dir"
echo "   ✅ mkdir succeeded"

# Test file creation
echo "   - Creating file /test_dir/hello.txt..."
touch "$MOUNT_POINT/test_dir/hello.txt"
echo "   ✅ touch succeeded"

# Test write (this was hanging before)
echo "   - Writing to file..."
echo "Hello, SlayerFS!" > "$MOUNT_POINT/test_dir/hello.txt"
echo "   ✅ write succeeded"

# Test read
echo "   - Reading from file..."
CONTENT=$(cat "$MOUNT_POINT/test_dir/hello.txt")
if [ "$CONTENT" != "Hello, SlayerFS!" ]; then
    echo "   ❌ Content mismatch: expected 'Hello, SlayerFS!', got '$CONTENT'"
    fusermount -u "$MOUNT_POINT"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi
echo "   ✅ read succeeded, content matches"

# Test ls
echo "   - Listing directory..."
ls -la "$MOUNT_POINT/test_dir/"
echo "   ✅ ls succeeded"

# Unmount
echo ""
echo "5. Unmounting filesystem..."
fusermount -u "$MOUNT_POINT"
wait $DEMO_PID 2>/dev/null || true
echo "✅ Unmounted successfully"

# Test persistence: remount and check data
echo ""
echo "6. Testing persistence (remounting)..."
$DEMO_BIN --config "$CONFIG_FILE" --mount "$MOUNT_POINT" --storage "$STORAGE_DIR" &
DEMO_PID=$!
sleep 2

if ! mountpoint -q "$MOUNT_POINT"; then
    echo "❌ Failed to remount filesystem"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi
echo "✅ Remounted successfully"

# Check if data persisted
echo "   - Checking if data persisted..."
if [ ! -f "$MOUNT_POINT/test_dir/hello.txt" ]; then
    echo "   ❌ File not found after remount"
    fusermount -u "$MOUNT_POINT"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi

CONTENT=$(cat "$MOUNT_POINT/test_dir/hello.txt")
if [ "$CONTENT" != "Hello, SlayerFS!" ]; then
    echo "   ❌ Content mismatch after remount: expected 'Hello, SlayerFS!', got '$CONTENT'"
    fusermount -u "$MOUNT_POINT"
    kill $DEMO_PID 2>/dev/null || true
    exit 1
fi
echo "   ✅ Data persisted correctly"

# Clean up
echo ""
echo "7. Cleaning up..."
fusermount -u "$MOUNT_POINT"
wait $DEMO_PID 2>/dev/null || true
rm -rf "$MOUNT_POINT" "$STORAGE_DIR"

echo ""
echo "🎉 All tests passed!"
echo ""
echo "Summary:"
echo "  ✅ Mount/unmount"
echo "  ✅ Create directory"
echo "  ✅ Create file"
echo "  ✅ Write data (fixed!)"
echo "  ✅ Read data"
echo "  ✅ List directory"
echo "  ✅ Data persistence"
