package datasync

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// s3Related checks if the data is operating s3 storage.
func s3Related(w *wrappedData) bool {
	logutil.Infof("liubo: data %v", w.data)
	head := objectio.DecodeIOEntryHeader(w.data)
	codec := objectio.GetIOEntryCodec(*head)
	entry, err := codec.Decode(w.data[4:])
	if err != nil {
		logutil.Error("failed to decode payload")
		return false
	}
	txnCmd, ok := entry.(*txnbase.TxnCmd)
	if !ok {
		logutil.Error("type convert error")
		return false
	}
	if txnCmd.ComposedCmd == nil {
		logutil.Error("composed cmd is nil")
		return false
	}
	for _, cmd := range txnCmd.ComposedCmd.Cmds {
		logutil.Infof("liubo: cmd type %s", cmd.String())
		/*
			if catalog.IOET_WALTxnCommand_Object == cmd.GetType() && cmd.Close() {
				return true
			}
		*/
	}
	return false
}
