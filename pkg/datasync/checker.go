package datasync

import (
	"context"
	"path/filepath"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	c1 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

const (
	checkpointDir = "ckp/"
)

type metaFile struct {
	index int
	start types.TS
	end   types.TS
	name  string
}

type checkpointLoader struct {
	ctx              context.Context
	fileService      fileservice.FileService
	checkpointSchema *catalog.Schema
}

func newCheckpointLoader(
	ctx context.Context, fileService fileservice.FileService,
) (*checkpointLoader, error) {
	l := &checkpointLoader{
		ctx:              ctx,
		fileService:      fileService,
		checkpointSchema: catalog.NewEmptySchema("checkpoint"),
	}
	for i, col := range checkpoint.CheckpointSchemaAttr {
		if err := l.checkpointSchema.AppendCol(col, checkpoint.CheckpointSchemaTypes[i]); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *checkpointLoader) Load() ([]*checkpoint.CheckpointEntry, error) {
	files, err := l.fileService.List(l.ctx, checkpointDir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	metaFiles := make([]*metaFile, 0)
	for i, file := range files {
		start, end := blockio.DecodeCheckpointMetadataFileName(file.Name)
		metaFiles = append(metaFiles, &metaFile{
			name:  file.Name,
			start: start,
			end:   end,
			index: i,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	file := files[targetIdx]
	reader, err := blockio.NewFileReader("", l.fileService, filepath.Join(checkpointDir, file.Name))
	if err != nil {
		return nil, err
	}
	bats, closeCB, err := reader.LoadAllColumns(l.ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := l.checkpointSchema.Attrs()
	colTypes := l.checkpointSchema.Types()
	var checkpointVersion int
	vecLen := len(bats[0].Vecs)
	if vecLen < checkpoint.CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < checkpoint.CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], c1.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], c1.CheckpointAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	entries, _ := checkpoint.ReplayCheckpointEntries(bat, checkpointVersion)

	return entries, nil
}
