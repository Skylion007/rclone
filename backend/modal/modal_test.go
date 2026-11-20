package modal

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/require"
)

func TestAbsoluteStaysInRoot(t *testing.T) {
	f := &Fs{root: "root"}
	require.Equal(t, "/", (&Fs{}).absolute(""))
	require.Equal(t, "/root", f.absolute(""))
	require.Equal(t, "/root/leaf", f.absolute("../leaf"))
}

// TestVolume tests the SDK's file operations on a disposable Volume.
func TestVolume(t *testing.T) {
	id := os.Getenv("RCLONE_MODAL_TEST_VOLUME_ID")
	if id == "" {
		t.Skip("set RCLONE_MODAL_TEST_VOLUME_ID to a disposable Volume")
	}
	python := os.Getenv("RCLONE_MODAL_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	ctx := context.Background()
	config := configmap.Simple{"volume_id": id, "python": python}
	f, err := NewFs(ctx, "modal-test", "rclone-integration", config)
	require.NoError(t, err)

	const remote = "nested/model*"
	const data = "0123456789"
	src := object.NewStaticObjectInfo(remote, time.Now(), -1, true, nil, nil)
	obj, err := f.Features().PutStream(ctx, strings.NewReader(data), src)
	require.NoError(t, err)
	t.Cleanup(func() { _ = obj.Remove(ctx) })
	require.EqualValues(t, len(data), obj.Size())
	require.ErrorIs(t, f.Rmdir(ctx, "nested"), fs.ErrorDirectoryNotEmpty)

	rootFile, err := NewFs(ctx, "modal-test", "rclone-integration/"+remote, config)
	require.ErrorIs(t, err, fs.ErrorIsFile)
	require.Equal(t, "rclone-integration/nested", rootFile.Root())

	var files []string
	err = f.Features().ListR(ctx, "", func(entries fs.DirEntries) error {
		for _, entry := range entries {
			if _, ok := entry.(fs.Object); ok {
				files = append(files, entry.Remote())
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, files, remote)

	obj, err = f.NewObject(ctx, remote)
	require.NoError(t, err)
	reader, err := obj.Open(ctx, &fs.RangeOption{Start: 2, End: 5})
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "2345", string(content))

	require.NoError(t, obj.Remove(ctx))
	_, err = f.NewObject(ctx, remote)
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
}
