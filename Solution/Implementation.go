package Solution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
)

type FloodControlImpl struct {
	db       *sql.DB
	boundary int64
}

func NewFloodControlImpl(db *sql.DB, boundary int64) *FloodControlImpl {
	return &FloodControlImpl{db: db, boundary: boundary}
}

func (f *FloodControlImpl) ChangeSecondsTo(seconds int) error {
	_, err := f.db.Exec(fmt.Sprintf("CREATE OR REPLACE FUNCTION TimeInsert(myUserId bigint) RETURNS BIGINT\n"+
		"AS $$\n"+
		"    DECLARE currentTime timestamp = now();\n"+
		"    begin\n"+
		"        INSERT INTO timesAndUsers(callTime, userId) VALUES (currentTime, myUserId);\n"+
		"        DELETE FROM timesAndUsers WHERE callTime < currentTime - interval '%d seconds';\n"+
		"        RETURN (SELECT totalCalls FROM Count WHERE userId = myUserId);\n"+
		"    end;\n"+
		"$$ LANGUAGE 'plpgsql';\n", seconds))
	return err
}

func (f *FloodControlImpl) Check(ctx context.Context, userID int64) (bool, error) {
	row := f.db.QueryRow("SELECT TimeInsert(cast(? as BIGINT));", userID)
	var total int64
	err := row.Scan(&total)
	if err != nil {
		return false, err
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return false, errors.New("context deadline exceeded")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return false, errors.New("operation canceled")
	}
	return total < f.boundary, nil
}
