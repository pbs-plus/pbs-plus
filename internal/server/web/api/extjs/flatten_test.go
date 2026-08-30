package extjs

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestBuildTargetTreeGroupsDatabaseTargets(t *testing.T) {
	tree := BuildTargetTree([]coredb.Target{
		{Name: "pg-main", Type: coredb.TargetTypePostgreSQL, DatabaseHost: "pg.internal", DatabasePort: 5432},
		{Name: "maria-main", Type: coredb.TargetTypeMySQL, DatabaseHost: "maria.internal", DatabasePort: 3306, DatabaseVariant: "mariadb"},
	})
	if len(tree) != 2 || tree[0].GroupType != "postgresql" || tree[1].GroupType != "mysql" {
		t.Fatalf("tree = %#v", tree)
	}
	if tree[0].Children[0].DatabaseHost != "pg.internal" || tree[1].Children[0].DatabaseVariant != "mariadb" {
		t.Fatalf("database target details missing: %#v", tree)
	}
}
