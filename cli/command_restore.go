package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	restoreCommandHelp = `Restore a directory or file from a snapshot into the specified target path.

By default, the target path will be created by the restore command if it does
not exist.

The source to be restored is specified in the form of a directory or file ID and
optionally a sub-directory path.

For example, the following source and target arguments will restore the contents
of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9' directory into a new, local directory
named 'd1'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will restore the contents of a subdirectory
'subdir/subdir2' under 'kffbb7c28ea6c34d6cbe555d1cf80faa9'  into a new, local
directory named 'sd2'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'
`
	restoreCommandSourcePathHelp = `Source directory ID/path in the form of a
directory ID and optionally a sub-directory path. For example,
'kffbb7c28ea6c34d6cbe555d1cf80faa9' or
'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'
`
)

var (
	restoreCommand              = app.Command("restore", restoreCommandHelp)
	restoreSourceID             = ""
	restoreTargetPath           = ""
	restoreOverwriteDirectories = true
	restoreOverwriteFiles       = true
	restoreConsistentAttributes = false
	restoreMode                 = restoreModeAuto
	restoreParallel             = 8
)

const (
	restoreModeLocal         = "local"
	restoreModeAuto          = "auto"
	restoreModeZip           = "zip"
	restoreModeZipNoCompress = "zip-nocompress"
	restoreModeTar           = "tar"
	restoreModeTgz           = "tgz"
)

func addRestoreFlags(cmd *kingpin.CmdClause) {
	cmd.Arg("source", restoreCommandSourcePathHelp).Required().StringVar(&restoreSourceID)
	cmd.Arg("target-path", "Path of the directory for the contents to be restored").Required().StringVar(&restoreTargetPath)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").BoolVar(&restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").BoolVar(&restoreOverwriteFiles)
	cmd.Flag("consistent-attributes", "When multiple snapshots match, fail if they have inconsistent attributes").Envar("KOPIA_RESTORE_CONSISTENT_ATTRIBUTES").BoolVar(&restoreConsistentAttributes)
	cmd.Flag("mode", "Override restore mode").EnumVar(&restoreMode, restoreModeAuto, restoreModeLocal, restoreModeZip, restoreModeZipNoCompress, restoreModeTar, restoreModeTgz)
	cmd.Flag("parallel", "Restore parallelism (1=disable)").IntVar(&restoreParallel)
}

func restoreOutput() (restore.Output, error) {
	p, err := filepath.Abs(restoreTargetPath)
	if err != nil {
		return nil, err
	}

	m := detectRestoreMode(restoreMode)
	switch m {
	case restoreModeLocal:
		return &restore.FilesystemOutput{
			TargetPath:           p,
			OverwriteDirectories: restoreOverwriteDirectories,
			OverwriteFiles:       restoreOverwriteFiles,
		}, nil

	case restoreModeZip, restoreModeZipNoCompress:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		method := zip.Deflate
		if m == restoreModeZipNoCompress {
			method = zip.Store
		}

		return restore.NewZipOutput(f, method), nil

	case restoreModeTar:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(f), nil

	case restoreModeTgz:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(gzip.NewWriter(f)), nil

	default:
		return nil, errors.Errorf("unknown mode %v", m)
	}
}

func detectRestoreMode(m string) string {
	if m != "auto" {
		return m
	}

	switch {
	case strings.HasSuffix(restoreTargetPath, ".zip"):
		printStderr("Restoring to a zip file (%v)...\n", restoreTargetPath)
		return restoreModeZip

	case strings.HasSuffix(restoreTargetPath, ".tar"):
		printStderr("Restoring to an uncompressed tar file (%v)...\n", restoreTargetPath)
		return restoreModeTar

	case strings.HasSuffix(restoreTargetPath, ".tar.gz") || strings.HasSuffix(restoreTargetPath, ".tgz"):
		printStderr("Restoring to a tar+gzip file (%v)...\n", restoreTargetPath)
		return restoreModeTgz

	default:
		printStderr("Restoring to local filesystem (%v) with parallelism=%v...\n", restoreTargetPath, restoreParallel)
		return restoreModeLocal
	}
}

func printRestoreStats(st restore.Stats) {
	printStderr("Restored %v files, %v directories and %v symbolic links (%v)\n", st.FileCount, st.DirCount, st.SymlinkCount, units.BytesStringBase10(st.TotalFileSize))
}

func runRestoreCommand(ctx context.Context, rep repo.Repository) error {
	output, err := restoreOutput()
	if err != nil {
		return errors.Wrap(err, "unable to initialize output")
	}

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, restoreSourceID, restoreConsistentAttributes)
	if err != nil {
		return errors.Wrap(err, "unable to get filesystem entry")
	}

	st, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
		Parallel: restoreParallel,
		ProgressCallback: func(enqueued, processing, completed int64) {
			log(ctx).Infof("Restored %v/%v. Processing %v...", completed, enqueued, processing)
		},
	})
	if err != nil {
		return err
	}

	printRestoreStats(st)

	return nil
}

func init() {
	addRestoreFlags(restoreCommand)
	restoreCommand.Action(repositoryAction(runRestoreCommand))
}
