package database

import (
	Dict "myredis/datastruct/dict"
	"myredis/protocol"
)

func (db *DB) getAsDict(key string) (Dict.Dict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}

}
