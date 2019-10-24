package autoinc

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

var autoID *UID

func TestMain(m *testing.M) {
	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/database")
	autoID, _ = New(db, "business", 100)

	os.Exit(m.Run())
}

func TestUID(t *testing.T) {
	for i := 0; i < 10; i++ {
		id, _ := autoID.Get()
		t.Logf("GetID: %d\n", id)
	}
}
