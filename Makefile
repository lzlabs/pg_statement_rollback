LIB = pg_statement_rollback

PGFILEDESC = "pg_statement_rollback - Automatic rollback at statement level for PostgreSQL"

PG_CONFIG = pg_config

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LDFLAGS = -L$(libpq_builddir) -lpq

SHLIB_LINK = $(libpq)

DOCS = $(wildcard README*)
MODULES = pg_statement_rollback

TESTS        = 01_slr_basic  \
	       02_slr_release \
	       03_slr_cursor \
	       04_slr_log_writeonly \
	       05_slr_write_cte \
	       06_slr_do_block

REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

