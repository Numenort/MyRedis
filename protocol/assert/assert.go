package assert

import (
	"fmt"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"runtime"
	"testing"
)

func printStack() string {
	_, file, no, ok := runtime.Caller(2)
	if ok {
		return fmt.Sprintf("at %s %d", file, no)
	}
	return ""
}

func AssertErrReply(t *testing.T, actual myredis.Reply, expected string) {
	errReply, ok := actual.(protocol.ErrorReply)
	if !ok {
		expectBytes := protocol.MakeErrReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectBytes) {
			return
		}
		t.Errorf("expected err protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if errReply.Error() != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

func AssertIntReply(t *testing.T, actual myredis.Reply, expected int) {
	intResult, ok := actual.(*protocol.IntReply)
	print(intResult, ok)
	if !ok {
		t.Errorf("expected int protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if intResult.Code != int64(expected) {
		t.Errorf("expected %d, actually %d, %s", expected, intResult.Code, printStack())
	}
}

func AssertBulkReply(t *testing.T, actual myredis.Reply, expected string) {
	bulkReply, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if !utils.BytesEquals(bulkReply.Arg, []byte(expected)) {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

func AssertStatusReply(t *testing.T, actual myredis.Reply, expected string) {
	statusReply, ok := actual.(*protocol.StatusReply)
	if !ok {
		expectedBytes := protocol.MakeStatusReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectedBytes) {
			return
		}
		t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if statusReply.Status != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

func AssertMultiBulkReply(t *testing.T, actual myredis.Reply, expected []string) {
	multiBulkReply, ok := actual.(*protocol.MultiBulkReply)
	if !ok {
		expectedArgs := make([][]byte, len(expected))
		for i, str := range expected {
			expectedArgs[i] = []byte(str)
		}
		expectedBytes := protocol.MakeMultiBulkReply(expectedArgs).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectedBytes) {
			return
		}
		t.Errorf("expected multi bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
	}
	if len(multiBulkReply.Args) != len(expected) {
		t.Errorf("expected %d elements, actually %d, %s",
			len(expected), len(multiBulkReply.Args), printStack())
		return
	}
	for i, v := range multiBulkReply.Args {
		str := string(v)
		if str != expected[i] {
			t.Errorf("expected %s, actually %s, %s", expected[i], actual, printStack())
		}
	}
}

func AssertMultiBulkReplySize(t *testing.T, actual myredis.Reply, expected int) {
	multiBulkReply, ok := actual.(*protocol.MultiBulkReply)
	if !ok {
		if expected == 0 &&
			utils.BytesEquals(actual.ToBytes(), protocol.MakeEmptyMultiBulkReply().ToBytes()) {
			return
		}
		t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if len(multiBulkReply.Args) != expected {
		t.Errorf("expected %d elements, actually %d, %s", expected, len(multiBulkReply.Args), printStack())
		return
	}
}
