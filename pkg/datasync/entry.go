// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datasync

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// s3Related checks if the data is operating s3 storage.
func s3Related(w *wrappedData) bool {
	// logutil.Infof("liubo: data %v", w.data)
	buf := w.data
	bbuf := bytes.NewBuffer(buf[4:])
	m := &logservicedriver.Meta{}
	m.ReadFrom(bbuf)
	for _, _ = range m.GetAddr() {
		e := entry.NewEmptyEntry()
		e.ReadFrom(bbuf)
		buf := e.Entry.GetPayload()
		head := objectio.DecodeIOEntryHeader(buf[:4])
		codec := objectio.GetIOEntryCodec(*head)
		entry, err := codec.Decode(buf[4:])
		if err != nil {
			panic(err)
		}
		logutil.Infof("lalala entry type is %T", entry)
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
	}
	return false
}
